/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "durability_monitor.h"
#include "stored-value.h"

#include <unordered_map>

/*
 * Represents a tracked SyncWrite.
 *
 * Note that:
 *
 * 1) We keep a reference of the pending SyncWrite StoredValue sitting in the
 *     HashTable.
 *
 * 2) So, we don't need to keep any pointer/reference
 *     to the Prepare queued_item sitting into the CheckpointManager,
 *     as the StoredValue contains all the data we need for enqueueing the
 *     Commit queued_item into the CheckpointManager when the Durability
 *     Requirement is met.
 *
 * @todo: Considering pros/cons of this approach versus storing a queued_item
 *     (ie, ref-counted object) from the CheckpointManager, maybe changing
 */
class DurabilityMonitor::SyncWrite {
public:
    SyncWrite(const StoredValue& sv, cb::durability::Requirements durReq)
        : sv(sv), durReq(durReq) {
    }

    const StoredValue& getStoredValue() const {
        return sv;
    }

    int64_t getBySeqno() const {
        return sv.getBySeqno();
    }

private:
    const StoredValue& sv;
    const cb::durability::Requirements durReq;
};

/*
 * Represents a VBucket Replication Chain in the ns_server meaning,
 * i.e. a list of replica nodes where the VBucket replicas reside.
 */
struct DurabilityMonitor::ReplicationChain {
    /**
     * @param replicaUUIDs ns_server-like set of replica ids, eg:
     *     {replica1, replica2, ..}
     */
    ReplicationChain(const std::vector<std::string>& replicaUUIDs,
                     const Container::iterator& it)
        : majority(replicaUUIDs.size() / 2 + 1) {
        for (auto uuid : replicaUUIDs) {
            memoryIterators[uuid] = it;
        }
    }

    // Index of replica iterators. The key is the replica UUID.
    // Each iterator points to the last SyncWrite ack'ed by the replica.
    // Note that the SyncWrite at iterator embeds the in-memory state of the
    // replica (via the SyncWrite seqno).
    // So, the DurabilityMonitor internal logic ensures that each iterator in
    // this map have only 2 possible states:
    // - points to Container::end(), if Container is empty
    // - points to an element of Container otherwise
    // I.e., an iterator is never singular.
    // That implies also that in the current DurabilityMonitor implementation
    // Container is empty only before the first SyncWrite is added for tracking.
    std::unordered_map<std::string, Container::iterator> memoryIterators;

    // Majority in the arithmetic definition: NumReplicas / 2 + 1
    const uint8_t majority;
};

DurabilityMonitor::DurabilityMonitor(VBucket& vb) : vb(vb) {
}

DurabilityMonitor::~DurabilityMonitor() = default;

ENGINE_ERROR_CODE DurabilityMonitor::registerReplicationChain(
        const std::vector<std::string>& replicaUUIDs) {
    if (replicaUUIDs.size() == 0) {
        throw std::logic_error(
                "DurabilityMonitor::registerReplicationChain: empty chain not "
                "allowed");
    }

    if (replicaUUIDs.size() > 1) {
        return ENGINE_ENOTSUP;
    }

    // Statically create a single RC. This will be expanded for creating
    // multiple RCs dynamically.
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    chain = std::make_unique<ReplicationChain>(replicaUUIDs,
                                               trackedWrites.container.begin());

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::addSyncWrite(
        const StoredValue& sv, cb::durability::Requirements durReq) {
    if (durReq.getLevel() != cb::durability::Level::Majority ||
        durReq.getTimeout() != 0) {
        return ENGINE_ENOTSUP;
    }
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    trackedWrites.container.push_back(SyncWrite(sv, durReq));
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::seqnoAckReceived(
        const std::string& replicaUUID, int64_t memorySeqno) {
    // @todo: The scope can be probably shorten. Deferring to follow-up patches
    //     as I'm amending this function considerably.
    std::lock_guard<std::mutex> lg(trackedWrites.m);

    // Note that in the current implementation of DurabilitMonitot Container
    // can be empty only before the first SyncWrite is added for tracking.
    if (trackedWrites.container.empty()) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: No tracked SyncWrite, "
                "but replica ack'ed memorySeqno:" +
                std::to_string(memorySeqno));
    }

    const auto next = getReplicaMemoryNext(replicaUUID);

    if (next == trackedWrites.container.end()) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: No pending SyncWrite, "
                "but replica ack'ed memorySeqno:" +
                std::to_string(memorySeqno));
    }

    int64_t pendingSeqno = next->getBySeqno();

    if (memorySeqno < pendingSeqno) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: Ack'ed seqno is behind "
                "pending seqno {ack'ed: " +
                std::to_string(memorySeqno) +
                ", pending:" + std::to_string(pendingSeqno) + "}");
    }
    // Note: not supporting any Replica ACK optimization yet
    if (memorySeqno > pendingSeqno) {
        return ENGINE_ENOTSUP;
    }

    // Update replica state, i.e. advance by 1
    advanceReplicaMemoryIterator(replicaUUID, 1);

    // Note: if we reach this point it is guaranteed that
    //     pendingSeqno==ACKseqno
    // So, in this first implementation (1 replica and no ACK optimization)
    // the Durability Requirement for the pending SyncWrite is implicitly
    // satisfied.

    // Commit the ack'ed SyncWrite
    commit(lg);

    return ENGINE_SUCCESS;
}

size_t DurabilityMonitor::getNumTracked() const {
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    return trackedWrites.container.size();
}

const DurabilityMonitor::Container::iterator&
DurabilityMonitor::getReplicaMemoryIterator(
        const std::string& replicaUUID) const {
    if (!chain) {
        throw std::logic_error(
                "DurabilityMonitor::getReplicaMemoryIterator: no chain "
                "registered");
    }
    const auto entry = chain->memoryIterators.find(replicaUUID);
    if (entry == chain->memoryIterators.end()) {
        throw std::invalid_argument(
                "DurabilityMonitor::getReplicaEntry: replicaUUID " +
                replicaUUID + " not found in chain");
    }
    return entry->second;
}

DurabilityMonitor::Container::iterator DurabilityMonitor::getReplicaMemoryNext(
        const std::string& replicaUUID) {
    const auto& it = getReplicaMemoryIterator(replicaUUID);
    // A replica iterator represent the durability state of a replica as seen
    // from the active and it is never singular:
    // 1) points to container.end()
    //     a) if container is empty, which cannot be the case here
    //     b) before the active has received the first ACK
    // 2) points to an element of container otherwise
    //
    // In the 1b) case the next iterator position is Container::begin.
    return (it == trackedWrites.container.end())
                   ? trackedWrites.container.begin()
                   : std::next(it);
}

void DurabilityMonitor::advanceReplicaMemoryIterator(
        const std::string& replicaUUID, size_t n) {
    // Note: complexity is constant as we use RandomAccessIterator
    auto& it = const_cast<DurabilityMonitor::Container::iterator&>(
            getReplicaMemoryIterator(replicaUUID));
    std::advance(it, n);
}

int64_t DurabilityMonitor::getReplicaMemorySeqno(
        const std::string& replicaUUID) const {
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    const auto& it = getReplicaMemoryIterator(replicaUUID);
    if (it == trackedWrites.container.end()) {
        return 0;
    }
    return it->getBySeqno();
}

void DurabilityMonitor::commit(const std::lock_guard<std::mutex>& lg) {
    // @todo: do commit.
    // Here we will:
    // 1) update the SyncWritePrepare to SyncWriteCommit in the HT
    // 2) enqueue a SyncWriteCommit item into the CM
    // 3) send a response with Success back to the client
}

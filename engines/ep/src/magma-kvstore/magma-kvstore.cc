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

#include "magma-kvstore.h"
#include "bucket_logger.h"
#include "collections/kvstore_generated.h"
#include "common.h"
#include "ep_time.h"
#include "magma-kvstore_config.h"
#include "vbucket.h"
#include "statwriter.h"
#include <flatbuffers/flatbuffers.h>

#include <string.h>
#include <algorithm>
#include <limits>

#include <stdio.h>
#include <cstdarg>

using namespace magma;

// Keys to localdb docs
static const Slice vbstateKey = {"_vbstate", 8};
static const Slice manifestKey = {"_local/collections/manifest", 27};
static const Slice openScopesKey = {"_local/scope/open", 17};
static const Slice openCollectionsKey = {"_local/collections/open", 23};
static const Slice droppedCollectionsKey = {"_local/collections/dropped", 26};

static void hexdump(const char* buf,
                    const int len,
                    std::string& hex,
                    std::string& ascii) {
    char tmp[10];

    hex.resize(0);
    ascii.resize(0);

    for (int i = 0; i < len; i++) {
        if (isprint(buf[i])) {
            ascii.append(&buf[i], 1);
        } else {
            ascii.append(" ", 1);
        }

        snprintf(tmp, sizeof(tmp), "%02x", static_cast<uint8_t>(buf[i]));
        hex.append(tmp, 2);
    }
}

struct kvstats_ctx {
    kvstats_ctx(Collections::VB::Flush& collectionsFlush)
        : collectionsFlush(collectionsFlush) {
    }
    // A map of key to bool. If true, the key exists in the VB datafile
    std::unordered_map<DiskDocKey, bool> keyStats;

    // Collection flusher data for managing manifest changes and item counts
    Collections::VB::Flush& collectionsFlush;
};

static uint64_t GetSeqNum(const Slice& metaSlice);
static Vbid GetVbid(const Slice& metaSlice);

namespace magmakv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to Magma and when reading a Document
// from Magma.
class MetaData {
public:
    MetaData()
        : bySeqno(0),
          cas(0),
          exptime(0),
          revSeqno(0),
          flags(0),
          valueSize(0),
          vbid(0),
          deleted(0),
          deleteSource(static_cast<uint8_t>(DeleteSource::Explicit)),
          version(0),
          datatype(0){};

    MetaData(const Item& it)
        : bySeqno(it.getBySeqno()),
          cas(it.getCas()),
          exptime(it.getExptime()),
          revSeqno(it.getRevSeqno()),
          flags(it.getFlags()),
          valueSize(it.getNBytes()),
          vbid(it.getVBucketId()),
          datatype(it.getDataType()) {
        if (it.isDeleted()) {
            deleted = 1;
            deleteSource = static_cast<uint8_t>(it.deletionSource());
        } else {
            deleted = 0;
        }

        if (it.getOperation() == queue_op::pending_sync_write) {
            op = static_cast<uint8_t>(it.getOperation());
            // Note: durabilty timeout /isn't/ persisted as part of a pending
            // SyncWrite. This is because if we ever read it back from disk
            // during warmup (i.e. the commit_sync_write was never persisted),
            // we don't know if the SyncWrite was actually already committed; as
            // such to ensure consistency the pending SyncWrite *must*
            // eventually commit (or sit in pending forever).
            level = it.getDurabilityReqs().getLevel();
        }
        version = 0;
    };

    MetaData(bool isDeleted,
             uint32_t valueSize,
             uint32_t exptime,
             int64_t seqno,
             Vbid vbid)
        : bySeqno(seqno),
          exptime(exptime),
          valueSize(valueSize),
          vbid(vbid),
          deleted(isDeleted ? 1 : 0) {
        if (isDeleted) {
            deleteSource = static_cast<uint8_t>(DeleteSource::Explicit);
        }
        cas = 0;
        revSeqno = 0;
        flags = 0;
        version = 0;
        datatype = 0;
        op = 0;
    };

    std::string to_string() const {
        std::stringstream ss;
        ss << "bySeqno:" << bySeqno << " cas:" << cas << " exptime:" << exptime
           << " revSeqno:" << revSeqno << " flags:" << flags << " valueSize "
           << valueSize << " " << vbid
           << " deleted:" << (deleted == 0 ? "false" : "true")
           << " deleteSource:"
           << (deleted == 0 ? " " : deleteSource == 0 ? "Explicit" : "TTL")
           << " version:" << uint8_t(version)
           << " datatype:" << uint8_t(datatype) << " operation:" << uint8_t(op)
           << " durability:" << uint8_t(level);
        return ss.str();
    }

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
    int64_t bySeqno;
    uint64_t cas;
    uint32_t exptime;
    uint64_t revSeqno;
    uint32_t flags;
    uint32_t valueSize;
    Vbid vbid;
    uint8_t deleted : 1;
    uint8_t deleteSource : 1;
    uint8_t version : 6;
    uint8_t datatype;
    uint8_t op;
    cb::durability::Level level;
#pragma pack()
};
} // namespace magmakv

/**
 * Class representing a document to be persisted in Magma.
 */
class MagmaRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param del Flag indicating if it is an item deletion or not
     */
    MagmaRequest(const Item& item,
                 MutationRequestCallback callback,
                 std::shared_ptr<BucketLogger> logger)
        : IORequest(item.getVBucketId(), std::move(callback), DiskDocKey{item}),
          docMeta(magmakv::MetaData(item)),
          docBody(item.getValue()) {
        keySlice = {reinterpret_cast<const char*>(key.data()), key.size()};
        logger->debug("Request {}", to_string().c_str());
    }

    magmakv::MetaData& getDocMeta() {
        return docMeta;
    }

    Vbid getVbID() const {
        return docMeta.vbid;
    }

    int64_t getBySeqno() const {
        return docMeta.bySeqno;
    }

    size_t getKeyLen() const {
        return keySlice.Len();
    }

    const char* getKeyData() const {
        return keySlice.Data();
    }

    size_t getBodySize() const {
        return docBody ? docBody->valueSize() : 0;
    }

    const void* getBodyData() const {
        return docBody ? docBody->getData() : nullptr;
    }

    size_t getMetaSize() const {
        return sizeof(magmakv::MetaData);
    }

    void markOldItemExists() {
        itemOldExists = true;
    }

    bool oldItemExists() const {
        return itemOldExists;
    }

    void markOldItemIsDelete() {
        itemOldIsDelete = true;
    }

    bool oldItemIsDelete() const {
        return itemOldIsDelete;
    }

    void markRequestFailed() {
        reqFailed = true;
    }

    bool requestFailed() const {
        return reqFailed;
    }

    std::string to_string() const {
        std::string hex, ascii;
        hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
        std::stringstream ss;
        ss << "Key(hex):" << hex << " Key(ascii):" << ascii
           << " docMeta:" << docMeta.to_string()
           << " itemOldExists:" << (itemOldExists ? "true" : "false")
           << " itemOldIsDelete:" << (itemOldIsDelete ? "true" : "false")
           << " reqFailed:" << (reqFailed ? "true" : "false");
        return ss.str();
    }

private:
    Slice keySlice;
    magmakv::MetaData docMeta;
    value_t docBody;
    bool itemOldExists = false;
    bool itemOldIsDelete = false;
    bool reqFailed = false;
};

// Helper functions to pull meta data from a metaSlice
static magmakv::MetaData* GetDocMeta(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta;
}

static uint64_t GetSeqNum(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->bySeqno;
}

static uint32_t GetExpiryTime(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->exptime;
}

static bool IsDeleted(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->deleted > 0 ? true : false;
}

static Vbid GetVbid(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->vbid;
}

static uint8_t GetDataType(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->datatype;
}

static DiskDocKey makeDiskDocKey(const Slice key) {
    return DiskDocKey{key.Data(), key.Len()};
}

/**
  CompactionCallback class is used as part of compaction. LSM compaction
  will remove duplicates appropriately and the rest of the keys get
  processed by this callback. Its function is to remove keys from dropped
  collections or expire the approprate keys.
 */
class MagmaCompactionCB : public Magma::CompactionCallback {
public:
    MagmaCompactionCB(MagmaKVStore* magmaKVStore) : magmaKVStore(magmaKVStore) {
        magmaKVStore->logger->debug("MagmaCompactionCB constructor");
        initialized = false;
    }
    ~MagmaCompactionCB() {
        if (initialized && ndeletes > 0) {
            auto magmaInfo = magmaKVStore->getMagmaInfo(vbid);
            magmaKVStore->logger->debug(
                    "MagmaCompactionCB destructor"
                    " docCount {} ndeletes {}",
                    magmaInfo->docCount,
                    ndeletes);
            magmaInfo->docCount -= ndeletes;
        } else {
            magmaKVStore->logger->debug("MagmaCompactionCB destructor");
        }
    }

    bool operator()(const Slice& keySlice,
                    const Slice& metaSlice,
                    const Slice& valueSlice) {
        std::stringstream itemString;
        if (magmaKVStore->logger->should_log(spdlog::level::debug)) {
            itemString << GetVbid(metaSlice).to_string();

            auto tmpDiskDocKey = makeDiskDocKey(keySlice);
            auto tmpcid = tmpDiskDocKey.getDocKey().getCollectionID();
            itemString << " cid:";
            if (tmpcid.isSystem()) {
                tmpcid = Collections::getCollectionIDFromKey(
                        tmpDiskDocKey.getDocKey());
                itemString << "(system)";
            }
            itemString << tmpcid;
            std::string hex, ascii;
            hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
            itemString << " key:'" << hex << "' '" << ascii << "'";
            auto docMeta = GetDocMeta(metaSlice);
            itemString << docMeta->to_string();
            // magmaKVStore->logger->debug("MagmaCompactionCB {}",
            // itemString.str());
        }

        vbid = GetVbid(metaSlice);

        if (!initialized) {
            {
                std::unique_lock<std::mutex> lock(
                        magmaKVStore->compactionCtxMutex);
                ctx = magmaKVStore->compaction_ctxList[vbid.get()];
            }

            // If no ctx has been setup yet, have to assume we can't do any
            // collections dropping or expiry. We have to wait for kv_engine
            // to call to setup the compactionCtx.
            initialized = true;
        }

        if (ctx) {
            auto seqno = GetSeqNum(metaSlice);
            auto isKeyDeleted = IsDeleted(metaSlice);
            if (ctx->droppedKeyCb) {
                auto key = makeDiskDocKey(keySlice);
                if (ctx->eraserContext->isLogicallyDeleted(key.getDocKey(),
                                                           seqno)) {
                    ctx->droppedKeyCb(key, seqno);
                    if (isKeyDeleted) {
                        ctx->stats.collectionsDeletedItemsPurged++;
                    } else {
                        ctx->stats.collectionsItemsPurged++;
                    }
                    magmaKVStore->logger->debug(
                            "MagmaCompactionCB DROP collection dropped {}",
                            itemString.str());
                    return DROP_ITEM;
                }
            }

            auto exptime = GetExpiryTime(metaSlice);
            Status status;

            if (IsDeleted(metaSlice)) {
                uint64_t maxSeqno;
                status = magmaKVStore->magma->GetMaxSeqno(vbid.get(), maxSeqno);
                if (!status) {
                    throw std::runtime_error(
                            "MagmaKVStore::CompactionCallback Failed : " +
                            status.String());
                }

                if (seqno != maxSeqno) {
                    // If drop_deletes is true, we must be doing a test.
                    if (ctx->compactConfig.drop_deletes != 0) {
                        magmaKVStore->logger->debug(
                                "MagmaCompactionCB DROP drop_deletes {}",
                                itemString.str());
                        ctx->stats.tombstonesPurged++;
                        if (seqno > ctx->max_purged_seq) {
                            ctx->max_purged_seq = seqno;
                        }
                        return DROP_ITEM;
                    }

                    if (exptime < ctx->compactConfig.purge_before_ts &&
                        (exptime ||
                         !ctx->compactConfig.retain_erroneous_tombstones) &&
                        (!ctx->compactConfig.purge_before_seq ||
                         seqno <= ctx->compactConfig.purge_before_seq)) {
                        magmaKVStore->logger->debug(
                                "MagmaCompactionCB DROP expired tombstone {}",
                                itemString.str());
                        ctx->stats.tombstonesPurged++;
                        if (seqno > ctx->max_purged_seq) {
                            ctx->max_purged_seq = seqno;
                        }
                        return DROP_ITEM;
                    }
                }
            } else {
                time_t currTime = ep_real_time();
                if (exptime && exptime < currTime) {
                    if (mcbp::datatype::is_xattr(GetDataType(metaSlice))) {
                        // TODO: handle compression
                    }
                    auto docMeta = GetDocMeta(metaSlice);
                    auto itm = std::make_unique<Item>(
                            makeDiskDocKey(keySlice).getDocKey(),
                            docMeta->flags,
                            docMeta->exptime,
                            nullptr,
                            0,
                            docMeta->datatype,
                            docMeta->cas,
                            docMeta->bySeqno,
                            vbid,
                            docMeta->revSeqno);
                    itm->setDeleted(DeleteSource::TTL);
                    ctx->expiryCallback->callback(*(itm.get()), currTime);
                    magmaKVStore->logger->debug(
                            "MagmaCompactionCB expiry callback {}",
                            itemString.str());
                }
            }
        }

        magmaKVStore->logger->debug("MagmaCompactionCB KEEP {}",
                                    itemString.str());
        return KEEP_ITEM;
    }

    bool initialized = false;
    std::shared_ptr<compaction_ctx> ctx;
    MagmaKVStore* magmaKVStore;
    Vbid vbid;
    size_t ndeletes = 0;
    bool KEEP_ITEM = false;
    bool DROP_ITEM = true;

    friend class MagmaKVStore;
};

using namespace std::chrono_literals;

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration)
    : KVStore(configuration),
      pendingReqs(std::make_unique<PendingRequestQueue>()),
      in_transaction(false),
      magmaPath(configuration.getDBName() + "/magma."),
      scanCounter(0) {
    size_t writeCacheSize;

    // Special case for testing where we want every SyncCommitBatch to
    // create a CommitPoint, we set the writeCacheSize to 0 to force
    // a memtableFlush.
    size_t memtablesQuota = configuration.getBucketQuota() /
                            configuration.getMaxShards() *
                            configuration.getMagmaMemQuotaRatio();
    if (configuration.getMagmaMaxWriteCache() == 0 &&
        configuration.getMagmaMinWriteCache() == 0) {
        writeCacheSize = 0;
    } else {
        // The writeCacheSize is based on the bucketQuota but also must be
        // between minWriteCacheSize(8MB) and maxWriteCacheSize(128MB).
        writeCacheSize =
                std::min(memtablesQuota, configuration.getMagmaMaxWriteCache());
        writeCacheSize =
                std::max(writeCacheSize, configuration.getMagmaMinWriteCache());
    }

    configuration.setUseUpsert(configuration.getUseUpsert());

    std::string basePath =
            "/data" + std::to_string(configuration.getShardId() % 4);
    // Magma path is unique per shard.
    configuration.cfg.Path =
            basePath + "/magma/" + std::to_string(configuration.getShardId());
    configuration.cfg.MaxKVStores = configuration.getMaxVBuckets();
    cachedVBStates.resize(configuration.getMaxVBuckets());
    cachedMagmaInfo.resize(configuration.getMaxVBuckets());
    compaction_ctxList.resize(configuration.getMaxVBuckets());
    configuration.cfg.MaxKVStoreLSDBufferSize =
            configuration.getMagmaLsdBufferSize();
    configuration.cfg.LSDFragmentationRatio =
            configuration.getMagmaLsdFragmentationRatio();
    configuration.cfg.MaxCommitPoints = configuration.getMagmaMaxCommitPoints();
    auto commitPointInterval = configuration.getMagmaCommitPointInterval();
    configuration.cfg.CommitPointInterval =
            commitPointInterval * std::chrono::milliseconds{1min};
    configuration.cfg.MinValueSize = configuration.getMagmaMinValueSize();
    configuration.cfg.MaxWriteCacheSize = writeCacheSize;
    configuration.cfg.WALBufferSize = configuration.getMagmaWalBufferSize();
    configuration.cfg.NumFlushers = configuration.getMagmaNumFlushers();
    configuration.cfg.NumCompactors = configuration.getMagmaNumCompactors();
    auto walSyncTime = configuration.getMagmaWalSyncInterval();
    configuration.cfg.WALSyncTime =
            walSyncTime * std::chrono::milliseconds{1ms};
    setBatchCommitPoint(configuration.getMagmaBatchCommitPoint());
    configuration.cfg.ExpiryFragThreshold =
            configuration.getMagmaExpiryFragThreshold();
    configuration.cfg.TombstoneFragThreshold =
            configuration.getMagmaTombstoneFragThreshold();

    configuration.cfg.GetSeqNum = GetSeqNum;
    configuration.cfg.GetExpiryTime = GetExpiryTime;
    configuration.cfg.IsTombstone = IsDeleted;
    if (configuration.shouldUseUpsert()) {
        configuration.cfg.EnableUpdateStatusForSet = true;
    } else {
        configuration.cfg.EnableUpdateStatusForSet = false;
    }

    configuration.cfg.DumpDebugStats = true;

    std::string loggerName =
            "magma_" + std::to_string(configuration.getShardId());
    logger = BucketLogger::createBucketLogger(loggerName, loggerName);
    /*
    configuration.cfg.LogContext = logger;

    configuration.cfg.MakeCompactionCallback = [&]() {
        return std::make_unique<MagmaCompactionCB>(this);
    };

    */
    configuration.cfg.LogLevel = "info";

    auto currEngine = ObjectRegistry::getCurrentEngine();
    configuration.magmaCfg.SetupThreadContext = [currEngine]() {
        ObjectRegistry::onSwitchThread(currEngine,
                                       false);
    };

    configuration.cfg.WriteCacheAllocationCallback = [&](size_t size,
                                                         bool alloc) {
        if (alloc) {
            ObjectRegistry::memoryAllocated(size);
        } else {
            ObjectRegistry::memoryDeallocated(size);
        }
    };

    createDataDir(configuration.getDBName());

    logger->critical(
            "magma constructor\n\tPath {}"
            "\n\tMaxKVStores {}"
            "\n\tMaxKVStoreLSDBufferSize {}"
            " LSDFragmentationRatio {}"
            "\n\tMaxCommitPoints {}"
            " CommitPointInterval {}m"
            " BatchCommitPoint {}"
            "\n\tMinValueSize {}"
            "\n\tBucketQuota {} MB"
            " # Shards {}"
            " MemQuotaRatio {}"
            " memTableQuota {}"
            "\n\tMaxWriteCacheSize {} MB"
            "\n\tWALBufferSize {} MB"
            "\n\tLSMNumMemtables {}"
            " LSMMaxSSTableSize {}MB"
            " LSMMaxBaseLevelSize {}MB"
            "\n\tLSMLevelSizeMultiplier {}"
            " SeqTreeBlockSize {}"
            "\n\tLSMMaxNumLevel0Tables {}"
            " LSMNumLevels {}"
            " LSDNumLevels {}"
            " LocalNumLevels {}"
            "\n\tNumFlushers {}"
            "\n\tNumCompactors {}"
            "\n\tWALSyncTime {} s"
            "\n\tExpiryFragThreshold {}"
            "\n\tTombstoneFragThreshold {}",
            configuration.cfg.Path,
            configuration.cfg.MaxKVStores,
            configuration.cfg.MaxKVStoreLSDBufferSize,
            configuration.cfg.LSDFragmentationRatio,
            configuration.cfg.MaxCommitPoints,
            int(configuration.cfg.CommitPointInterval /
                std::chrono::milliseconds{1min}),
            getBatchCommitPoint(),
            configuration.cfg.MinValueSize,
            configuration.getBucketQuota() / 1024 / 1024,
            configuration.getMaxShards(),
            configuration.getMagmaMemQuotaRatio(),
            memtablesQuota / 1024 / 1024,
            configuration.cfg.MaxWriteCacheSize / 1024 / 1024,
            configuration.cfg.WALBufferSize / 1024 / 1024,
            configuration.cfg.LSMNumMemtables,
            configuration.cfg.LSMMaxSSTableSize / 1024 / 1024,
            configuration.cfg.LSMMaxBaseLevelSize / 1024 / 1024,
            configuration.cfg.LSMSizeMultiplierBaseLevel,
            configuration.cfg.SeqTreeBlockSize,
            configuration.cfg.LSMMaxNumLevel0Tables,
            configuration.cfg.LSMNumLevels,
            configuration.cfg.LSDNumLevels,
            configuration.cfg.LocalNumLevels,
            configuration.cfg.NumFlushers,
            configuration.cfg.NumCompactors,
            int(configuration.cfg.WALSyncTime / std::chrono::milliseconds{1ms}),
            configuration.cfg.ExpiryFragThreshold,
            configuration.cfg.TombstoneFragThreshold);

    magma = std::make_unique<Magma>(configuration.cfg);
    magma->Open();
    auto kvstoreList = magma->GetKVStoreList();
    for (auto& kvid : kvstoreList) {
        Vbid vbid(kvid);
        readVBState(vbid);
    }
}

MagmaKVStore::~MagmaKVStore() {
    if (!in_transaction) {
        magma->Sync();
    }
    logger->debug("MagmaKVStore destructor");
}

std::string MagmaKVStore::getVBDBSubdir(Vbid vbid) {
    return magmaPath + "/" + std::to_string(vbid.get());
}

bool MagmaKVStore::begin(std::unique_ptr<TransactionContext> txCtx) {
    in_transaction = true;
    transactionCtx = std::move(txCtx);
    return in_transaction;
}

bool MagmaKVStore::commit(Collections::VB::Flush& collectionsFlush) {
    bool success = true;

    // This behaviour is to replicate the one in Couchstore.
    // If `commit` is called when not in transaction, just return true.
    if (!in_transaction) {
        logger->debug("MagmaKVStore::commit no txn");
        return success;
    }

    // If there are no pending IORequests and no collection manifset chagnes,
    // we are done.
    if (pendingReqs->size() == 0) {
        in_transaction = false;
        logger->debug("MagmaKVStore::commit no requests");
        return success;
    }

    kvstats_ctx kvctx(collectionsFlush);

    auto vbid = pendingReqs->front().getVbID();

    // Flush all documents to disk
    auto status = saveDocs(vbid, collectionsFlush, kvctx);
    if (status) {
        logger->warn(
                "MagmaKVStore::commit: saveDocs error:{}, "
                "vb:{}",
                status,
                vbid.to_string());
        success = false;
    }

    commitCallback(status, kvctx, vbid);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
        transactionCtx.reset();
    }

    pendingReqs->clear();
    logger->debug("MagmaKVStore::commit success:{}", success);
    return success;
}

void MagmaKVStore::commitCallback(int status, kvstats_ctx& kvctx, Vbid vbid) {
    for (const auto& req : *pendingReqs) {
        if (status) {
            ++st.numSetFailure;
        }

        size_t mutationSize =
                req.getKeyLen() + req.getBodySize() + req.getMetaSize();
        st.io_num_write++;
        st.io_write_bytes += mutationSize;

        int rv = DOC_NOT_FOUND;
        if (req.oldItemExists() && !req.oldItemIsDelete()) {
            rv = MUTATION_SUCCESS;
        }

        if (req.isDelete()) {
            if (req.requestFailed()) {
                st.numDelFailure++;
            } else {
                st.delTimeHisto.add(req.getDelta());
            }

            logger->debug("commitCallback {} getDelCallback rv:{}", vbid, rv);

            req.getDelCallback()(*transactionCtx, rv);
        } else {
            int mutationStatus = MUTATION_SUCCESS;
            if (req.oldItemIsDelete()) {
                rv = DOC_NOT_FOUND;
            }

            if (req.requestFailed()) {
                st.numSetFailure++;
                mutationStatus = MUTATION_FAILED;
            } else {
                st.writeTimeHisto.add(req.getDelta());
                st.writeSizeHisto.add(mutationSize);
            }
            mutation_result p(mutationStatus,
                              rv == DOC_NOT_FOUND ? true : false);
            req.getSetCallback()(*transactionCtx, p);

            logger->debug("commitCallback {} getSetCallback rv:{} insertion:{}",
                          vbid,
                          rv,
                          mutationStatus);
        }
    }
}

void MagmaKVStore::rollback() {
    logger->debug("MagmaKVStore::rollback in_transaction{}", in_transaction);
    if (in_transaction) {
        in_transaction = false;
        transactionCtx.reset();
    }
}

StorageProperties MagmaKVStore::getStorageProperties() {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

std::vector<vbucket_state*> MagmaKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void MagmaKVStore::set(const Item& item, SetCallback cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::set: in_transaction must be true to perform a "
                "set operation.");
    }
    pendingReqs->emplace_back(item, std::move(cb), logger);
}

GetValue MagmaKVStore::get(const DiskDocKey& key, Vbid vb, bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue MagmaKVStore::getWithHeader(void* dbHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly,
                                     bool fetchDelete) {
    auto start = std::chrono::steady_clock::now();
    Slice keySlice = {reinterpret_cast<const char*>(key.data()), key.size()};
    Slice meta;
    Slice value = {nullptr, 0};
    Magma::FetchBuffer idxBuf;
    Magma::FetchBuffer seqBuf;
    bool found;
    Status status =
            magma->Get(vb.get(), keySlice, idxBuf, seqBuf, meta, value, found);

    if (logger->should_log(spdlog::level::debug)) {
        std::string hex, ascii;
        hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
        logger->debug("getWithHeader {} key:{} {} status:{} found:{}",
                      vb,
                      ascii,
                      hex,
                      status.String(),
                      found);
    }

    if (!status) {
        logger->warn(
                "MagmaKVStore::getWithHeader: magma::DB::Lookup error:{}, "
                "vb:{}",
                status.ErrorCode(),
                vb);
    }
    if (!found) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }

#if 0
    //Fetch delete is never used in couch-kvstore.cc
    if (IsDeleted(meta) && !fetchDelete) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }
#endif

    std::string val{meta.Data(), meta.Len()};
    val.append(value.Data(), value.Len());

    // record stats
    st.readTimeHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));
    st.readSizeHisto.add(keySlice.Len() + meta.Len() + value.Len());

    return makeGetValue(vb, key, val, getMetaOnly);
}

void MagmaKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
    for (auto& it : itms) {
        auto& key = it.first;
        Slice keySlice = {reinterpret_cast<const char*>(key.data()),
                          key.size()};
        Slice meta;
        Slice value = {nullptr, 0};
        Magma::FetchBuffer idxBuf;
        Magma::FetchBuffer seqBuf;
        bool found;

        Status status = magma->Get(
                vb.get(), keySlice, idxBuf, seqBuf, meta, value, found);

        if (logger->should_log(spdlog::level::debug)) {
            std::string hex, ascii;
            hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
            logger->debug("getMulti {} key:{} {} status:{} found:{}",
                          vb,
                          ascii,
                          hex,
                          status.String(),
                          found);
        }

        if (found) {
            std::string val{meta.Data(), meta.Len()};
            val.append(value.Data(), value.Len());
            it.second.value = makeGetValue(vb, key, val, it.second.isMetaOnly);
            GetValue* rv = &it.second.value;
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value = rv;
            }
        } else {
            if (!status) {
                logger->warn(
                        "MagmaKVStore::getMulti: magma::DB::Lookup error:{}, "
                        "vb:{}",
                        status.ErrorCode(),
                        vb);
            }
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value->setStatus(ENGINE_KEY_ENOENT);
            }
        }
    }
}

/**
 * Just delete the old vbucket. It will get created on the
 * the next Set call.
 */
void MagmaKVStore::reset(Vbid vbid) {
    logger->debug("reset {}", vbid);

    if (getVBState(vbid)) {
        auto magmaInfo = getMagmaInfo(vbid);
        delVBucket(vbid, magmaInfo->fileRev);
    }
}

void MagmaKVStore::del(const Item& item, KVStore::DeleteCallback cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::del: in_transaction must be true to perform a "
                "delete operation.");
    }
    pendingReqs->emplace_back(item, std::move(cb), logger);
}

void MagmaKVStore::delVBucket(Vbid vbid, uint64_t vb_version) {
    logger->debug("delVBucket {} vb_version:{}", vbid, vb_version);

    if (getVBState(vbid)) {
        auto magmaInfo = getMagmaInfo(vbid);

        if (magmaInfo->fileRev >= vb_version) {
            auto status = magma->DeleteKVStore(vbid.get());
            if (!status) {
                logger->warn(
                        "MagmaKVStore::delVBucket failed "
                        "vbid {} Reason {}",
                        vbid.to_string(),
                        status.String());
            } else {
                cachedVBStates[vbid.get()] = nullptr;
                cachedMagmaInfo[vbid.get()] = nullptr;
            }
        }
    }
}

bool MagmaKVStore::snapshotVBucket(Vbid vbid,
                                   const vbucket_state& newvbstate,
                                   VBStatePersist options) {
    auto start = std::chrono::steady_clock::now();

    bool persist = false;
    if (updateCachedVBState(vbid, newvbstate) &&
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        if (!writeVBState(vbid, options)) {
            logger->warn(
                    "MagmaKVStore::snapshotVBucket: {} writeVBState failed {}",
                    vbid,
                    VBucket::toString(newvbstate.state));
            return false;
        }
        persist = true;
    }

    if (logger->should_log(spdlog::level::debug)) {
        nlohmann::json vbstateJSON;
        encodeVBState(getVBState(vbid), getMagmaInfo(vbid), vbstateJSON);
        logger->debug("snapshotVBucket {} writeVBState {} vbstate {}",
                      vbid,
                      persist,
                      vbstateJSON.dump());
    }

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

void MagmaKVStore::saveVBState(Vbid vbid, Magma::CommitBatch* batch) {
    // Convert vbstate & magmaInfo into json string
    nlohmann::json vbstateJSON;
    encodeVBState(getVBState(vbid), getMagmaInfo(vbid), vbstateJSON);
    std::string jstr = vbstateJSON.dump();

    logger->debug("saveVBState {} vbstate:{}", vbid, jstr);
    writeLocalDoc(batch, vbstateKey, jstr, false);
}

bool MagmaKVStore::writeVBState(Vbid vbid, VBStatePersist options) {
    if (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
        options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT) {
        if (options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT) {
            std::unique_ptr<Magma::CommitBatch> batch;
            Status status = magma->NewCommitBatch(vbid.get(), batch);
            if (!status) {
                logger->warn(
                        "MagmaKVStore::writeVBState failed creating batch for "
                        "vbid: {} err {}",
                        vbid.to_string(),
                        status.String());
                return false;
            }

            saveVBState(vbid, batch.get());

            status = magma->ExecuteCommitBatch(std::move(batch));
            if (!status) {
                logger->warn(
                        "MagmaKVStore::writeVBState: magma::ExecuteCommitBatch "
                        "error",
                        "vb:{}",
                        status.ErrorCode(),
                        vbid.to_string());
                return false;
            }

            status = magma->SyncCommitBatches(getBatchCommitPoint());
            if (!status) {
                logger->warn(
                        "MagmaKVStore::writeVBState: magma::SyncCommitBatches "
                        "error",
                        "vb:{}",
                        status.ErrorCode(),
                        vbid.to_string());
                return false;
            }
        }
    } else {
        throw std::invalid_argument(
                "MagmaKVStore::writeVBState: invalid vb state "
                "persist option specified for " +
                vbid.to_string());
        return false;
    }
    return true;
}

size_t MagmaKVStore::getNumShards() const {
    return configuration.getMaxShards();
}

std::unique_ptr<Item> MagmaKVStore::makeItem(Vbid vb,
                                             const DiskDocKey& key,
                                             const std::string& value,
                                             GetMetaOnly getMetaOnly) {
    Expects(value.size() >= sizeof(magmakv::MetaData));

    const char* data = value.c_str();

    magmakv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

    auto item = std::make_unique<Item>(key.getDocKey(),
                                       meta.flags,
                                       meta.exptime,
                                       includeValue ? data : nullptr,
                                       includeValue ? meta.valueSize : 0,
                                       meta.datatype,
                                       meta.cas,
                                       meta.bySeqno,
                                       vb,
                                       meta.revSeqno);

    if (meta.deleted > 0) {
        item->setDeleted(static_cast<DeleteSource>(meta.deleteSource));
    }

    return item;
}

GetValue MagmaKVStore::makeGetValue(Vbid vb,
                                    const DiskDocKey& key,
                                    const std::string& value,
                                    GetMetaOnly getMetaOnly) {
    return GetValue(
            makeItem(vb, key, value, getMetaOnly), ENGINE_SUCCESS, -1, 0);
}

vbucket_state* MagmaKVStore::initVBState(const Vbid vbid) {
    auto vbs = std::make_unique<vbucket_state>();
    memset((char*)vbs.get(), 0, sizeof(vbucket_state));
    vbs->state = vbucket_state_dead;
    vbs->hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    vbs->supportsNamespaces = true;
    cachedVBStates[vbid.get()] = std::move(vbs);

    auto magmaInfo = std::make_unique<MagmaInfo>();
    memset((char*)magmaInfo.get(), 0, sizeof(MagmaInfo));
    cachedMagmaInfo[vbid.get()] = std::move(magmaInfo);

    logger->debug("initVBState {}", vbid);

    return cachedVBStates[vbid.get()].get();
}

vbucket_state* MagmaKVStore::getVBState(const Vbid vbid) {
    auto vbstate = cachedVBStates[vbid.get()].get();
    if (vbstate) {
        if (logger->should_log(spdlog::level::debug)) {
            nlohmann::json vbstateJSON;
            encodeVBState(vbstate, getMagmaInfo(vbid), vbstateJSON);
            logger->debug(
                    "getVBState {} vbstate: {}", vbid, vbstateJSON.dump());
        }
        return vbstate;
    } else {
        logger->debug("getVBState {} empty", vbid);
        return nullptr;
    }
}

MagmaInfo* MagmaKVStore::getMagmaInfo(const Vbid vbid) {
    auto magmaInfo = cachedMagmaInfo[vbid.get()].get();
    if (!magmaInfo) {
        auto mi = std::make_unique<MagmaInfo>();
        memset((char*)mi.get(), 0, sizeof(MagmaInfo));
        cachedMagmaInfo[vbid.get()] = std::move(mi);
        magmaInfo = cachedMagmaInfo[vbid.get()].get();
    }
    return magmaInfo;
}

// encode the vbstate and magma dbinfo into json doc for storing
// in the local db.
void MagmaKVStore::encodeVBState(const vbucket_state* vbstate,
                                 const MagmaInfo* magmaInfo,
                                 nlohmann::json& vbstateJSON) {
    Status status;

    if (vbstate) {
        vbstateJSON["state"] = VBucket::toString(vbstate->state);
        vbstateJSON["checkpoint_id"] = vbstate->checkpointId;
        vbstateJSON["max_deleted_seqno"] =
                static_cast<uint64_t>(vbstate->maxDeletedSeqno);
        vbstateJSON["high_seqno"] = vbstate->highSeqno;
        vbstateJSON["purge_seqno"] = vbstate->purgeSeqno;
        vbstateJSON["snap_start"] = vbstate->lastSnapStart;
        vbstateJSON["snap_end"] = vbstate->lastSnapEnd;
        vbstateJSON["max_cas"] = vbstate->maxCas;
        vbstateJSON["hlc_epoch"] = vbstate->hlcCasEpochSeqno;
        vbstateJSON["might_contain_xattrs"] =
                vbstate->mightContainXattrs ? true : false;
        if (!vbstate->failovers.empty()) {
            vbstateJSON["failover_table"] = vbstate->failovers;
        }
        vbstateJSON["namespaces_supported"] = vbstate->supportsNamespaces;
    }
    if (magmaInfo) {
        vbstateJSON["doc_count"] = magmaInfo->docCount;
        vbstateJSON["persisted_deletes"] = magmaInfo->persistedDeletes;
    }
}

// decode the vbstate and magma dbinfo from the local db
Status MagmaKVStore::decodeVBState(const Vbid vbid,
                                   const std::string& valString) {
    auto vbstate = getVBState(vbid);
    if (!vbstate) {
        vbstate = initVBState(vbid);
    }
    auto magmaInfo = getMagmaInfo(vbid);

    std::string errStr = "MagmaKVStore::decodeVBState failed - vbucket( " +
                         std::to_string(vbid.get()) + ") ";

    nlohmann::json vbstateJSON;

    try {
        vbstateJSON = nlohmann::json::parse(valString);
    } catch (const nlohmann::json::exception& e) {
        return Status(errStr + " Failed to parse the vbstate json doc: " +
                      valString + ". Reason: " + e.what());
    }

    uint64_t defaultUint64 = 0;
    int64_t defaultInt64 = 0;

    auto vb_state = vbstateJSON.value("state", "");
    vbstate->checkpointId = vbstateJSON.value("checkpoint_id", defaultUint64);
    vbstate->maxDeletedSeqno =
            vbstateJSON.value("max_deleted_seqno", defaultUint64);

    if (vb_state.empty()) {
        return Status(errStr + " JSON doc for is in the wrong format: " +
                      valString + " vb state: " + vb_state +
                      " checkpoint id: " +
                      std::to_string(vbstate->checkpointId) +
                      " and max deleted seqno:" +
                      std::to_string(vbstate->maxDeletedSeqno));
    } else {
        vbstate->state = VBucket::fromString(vb_state.c_str());
        vbstate->highSeqno = vbstateJSON.value("high_seqno", defaultInt64);
        vbstate->purgeSeqno = vbstateJSON.value("purge_seqno", defaultUint64);
        vbstate->lastSnapStart = vbstateJSON.value("snap_start", defaultUint64);
        vbstate->lastSnapEnd = vbstateJSON.value("snap_end", defaultUint64);
        vbstate->maxCas = vbstateJSON.value("max_cas", defaultUint64);
        vbstate->supportsNamespaces =
                vbstateJSON.value("namespaces_supported", true);

        // MB-17517: If the maxCas on disk was invalid then don't use it -
        // instead rebuild from the items we load from disk (i.e. as per
        // an upgrade from an earlier version).
        if (vbstate->maxCas == static_cast<uint64_t>(-1)) {
            logger->warn(
                    "MagmaKVStore::decodeVBState: Invalid "
                    "vbstate.maxCas; max_cas set to 0");
            vbstate->maxCas = 0;
        }

        vbstate->hlcCasEpochSeqno =
                vbstateJSON.value("hlc_epoch", defaultInt64);
        vbstate->mightContainXattrs =
                vbstateJSON.value("might_contain_xattrs", false);
        std::string fo = vbstateJSON.value("failover_table", "");
        if (!fo.empty()) {
            vbstate->failovers = fo;
        }
    }

    magmaInfo->docCount = vbstateJSON.value("doc_count", defaultUint64);
    magmaInfo->persistedDeletes =
            vbstateJSON.value("persisted_deletes", defaultUint64);

    return Status::OK();
}
// Read vbstate from local db and place in cache.
Status MagmaKVStore::readVBState(const Vbid vbid) {
    std::string valString;
    auto status = readLocalDoc(vbid, vbstateKey, valString);
    if (!valString.empty()) {
        logger->debug("readVBState {} vbstate:{}", vbid, valString);
        auto vbstate = getVBState(vbid);
        if (!vbstate) {
            vbstate = initVBState(vbid);
        }
        status = decodeVBState(vbid, valString);
    }
    return status;
}

int MagmaKVStore::saveDocs(Vbid vbid,
                           Collections::VB::Flush& collectionsFlush,
                           kvstats_ctx& kvctx) {
    uint64_t ninserts = 0;
    uint64_t ndeletes = 0;

    int64_t lastSeqno = 0;
    std::unique_ptr<Magma::CommitBatch> batch;
    Status status = magma->NewCommitBatch(vbid.get(), batch);
    if (!status) {
        logger->warn(
                "MagmaKVStore::saveDocs failed creating batch for vbid:{} err: "
                "{}",
                vbid.to_string(),
                status.String());
        return status.ErrorCode();
    }

    auto begin = std::chrono::steady_clock::now();

    for (auto& req : *pendingReqs) {
        Slice key = Slice{req.getKeyData(), req.getKeyLen()};
        auto docMeta = req.getDocMeta();
        Slice meta = Slice{reinterpret_cast<char*>(&docMeta),
                           sizeof(magmakv::MetaData)};
        Slice value = Slice{
                reinterpret_cast<char*>(const_cast<void*>(req.getBodyData())),
                req.getBodySize()};

        bool found{false};
        bool tombstone{false};
        if (configuration.shouldUseUpsert()) {
            status = batch->Set(key, meta, value, &found, &tombstone);
            if (!status) {
                logger->warn("MagmaKVStore::saveDocs: Set {} error {}",
                             vbid.to_string(),
                             status.String());
                req.markRequestFailed();
                continue;
            }
        } else {
            Magma::FetchBuffer idxBuf;
            Slice oldMeta;
            status = magma->KeyExists(vbid.get(), key, idxBuf, oldMeta, found);
            if (!status) {
                std::string hex, ascii;
                hexdump(key.Data(), key.Len(), hex, ascii);
                logger->warn(
                        "MagmaKVStore::saveDocs: KeyExists {} "
                        "key:{} {} error {}",
                        vbid.to_string(),
                        hex,
                        ascii,
                        status.String());
                req.markRequestFailed();
                continue;
            }

            if (found && IsDeleted(oldMeta)) {
                tombstone = true;
            }

            status = batch->Set(key, meta, value);
            if (!status) {
                std::string hex, ascii;
                hexdump(key.Data(), key.Len(), hex, ascii);
                logger->warn(
                        "MagmaKVStore::saveDocs: Set {} key:{} {} error {}",
                        vbid.to_string(),
                        hex,
                        ascii,
                        status.String());
                req.markRequestFailed();
                continue;
            }
        }

        if (logger->should_log(spdlog::level::debug)) {
            std::string hex, ascii;
            hexdump(key.Data(), key.Len(), hex, ascii);
            logger->debug(
                    "saveDocs {} key:{} {} seqno:{} delete:{} found:{} "
                    "tombstone:{}",
                    vbid,
                    ascii,
                    hex,
                    req.getBySeqno(),
                    req.isDelete(),
                    found,
                    tombstone);
        }

        if (found) {
            req.markOldItemExists();
            if (tombstone) {
                req.markOldItemIsDelete();
                // Old item is a delete and new is an insert.
                if (!req.isDelete()) {
                    ninserts++;
                }
            } else if (req.isDelete()) {
                // Old item is insert and new is delete.
                ndeletes++;
            }
        } else {
            // Old item doesn't exist and new is an insert.
            if (!req.isDelete()) {
                ninserts++;
            }
        }

        auto collectionsKey = makeDiskDocKey(key);
        kvctx.keyStats[collectionsKey] = false;

        if (req.oldItemExists()) {
            if (!req.oldItemIsDelete()) {
                // If we are replacing the item...
                auto itr = kvctx.keyStats.find(collectionsKey);
                if (itr != kvctx.keyStats.end()) {
                    itr->second = true;
                }
            }
        }

        if (collectionsKey.isCommitted()) {
            if (req.oldItemExists()) {
                if (!req.oldItemIsDelete()) {
                    if (req.isDelete()) {
                        // Deleting the item
                        kvctx.collectionsFlush.decrementDiskCount(
                                collectionsKey.getDocKey());
                    }
                } else {
                    if (!req.isDelete()) {
                        // Old item is a delete and new is an insert.
                        kvctx.collectionsFlush.incrementDiskCount(
                                collectionsKey.getDocKey());
                    }
                }
            } else {
                // Item doesn't exist and isn't a delete... its a new item.
                if (!req.isDelete()) {
                    kvctx.collectionsFlush.incrementDiskCount(
                            collectionsKey.getDocKey());
                }
            }

            kvctx.collectionsFlush.setPersistedHighSeqno(
                    collectionsKey.getDocKey(),
                    req.getBySeqno(),
                    req.isDelete());

            if (req.getBySeqno() > lastSeqno) {
                lastSeqno = req.getBySeqno();
            }
        }
    }

    st.saveDocsHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));

    collectionsFlush.saveCollectionStats(
            std::bind(&MagmaKVStore::saveCollectionStats,
                      this,
                      std::ref(*batch),
                      std::placeholders::_1,
                      std::placeholders::_2));

    auto vbstate = getVBState(vbid);
    if (vbstate == nullptr) {
        vbstate = initVBState(vbid);
    }
    vbstate->highSeqno = lastSeqno;

    auto magmaInfo = getMagmaInfo(vbid);
    magmaInfo->docCount += ninserts - ndeletes;
    magmaInfo->persistedDeletes += ndeletes;

    // The vstate on disk will include both vbucket_state and
    // magmaInfo.
    saveVBState(vbid, batch.get());

    if (collectionsMeta.needsCommit) {
        status = updateCollectionsMeta(vbid, batch.get(), collectionsFlush);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: updateCollectionsMeta error:{}",
                    status.String());
        }
    }

    status = magma->ExecuteCommitBatch(std::move(batch));
    if (!status) {
        logger->warn("MagmaKVStore::saveDocs: ExecuteCommitBatch {} error {}",
                     vbid.to_string(),
                     status.ErrorCode());
    } else {
        status = magma->SyncCommitBatches(getBatchCommitPoint());
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: SyncCommitBatches {} error {}",
                    vbid.to_string(),
                    status.ErrorCode());
        }
    }

    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));

    return status.ErrorCode();
}

ScanContext* MagmaKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;

    auto vbstate = getVBState(vbid);
    if (!vbstate) {
        return nullptr;
    }

    auto magmaInfo = getMagmaInfo(vbid);
    auto collectionsManifest = getDroppedCollections(vbid);

    if (logger->should_log(spdlog::level::debug)) {
        std::string docFilter;
        switch (options) {
        case DocumentFilter::ALL_ITEMS:
            docFilter = "ALL_ITEMS";
            break;
        case DocumentFilter::NO_DELETES:
            docFilter = "NO_DELETES";
            break;
        default:
            docFilter = "-";
        }
        std::string valFilter;
        switch (valOptions) {
        case ValueFilter::KEYS_ONLY:
            valFilter = "KEYS_ONLY";
            break;
        case ValueFilter::VALUES_COMPRESSED:
            valFilter = "VALUES_COMPRESSED";
            break;
        case ValueFilter::VALUES_DECOMPRESSED:
            valFilter = "VALUES_DECOMPRESSED";
            break;
        default:
            valFilter = "-";
        }

        logger->debug(
                "initScanContext {} seqno:{} endSeqno:{}"
                " purgeSeqno:{} docCount:{} docFilter:{} valFilter:{}",
                vbid,
                startSeqno,
                vbstate->highSeqno,
                vbstate->purgeSeqno,
                magmaInfo->docCount,
                docFilter,
                valFilter);
    }

    auto sctx = new ScanContext(cb,
                                cl,
                                vbid,
                                scanId,
                                startSeqno,
                                vbstate->highSeqno,
                                vbstate->purgeSeqno,
                                options,
                                valOptions,
                                magmaInfo->docCount,
                                configuration,
                                collectionsManifest);
    sctx->logger = logger.get();
    return sctx;
}

scan_error_t MagmaKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        logger->debug("scan {} lastReadSeqno:{} == maxSeqno:{}",
                      ctx->vbid,
                      ctx->lastReadSeqno,
                      ctx->maxSeqno);
        return scan_success;
    }

    auto startSeqno = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        startSeqno = ctx->lastReadSeqno + 1;
    }

    GetMetaOnly isMetaOnly = ctx->valFilter == ValueFilter::KEYS_ONLY
                                     ? GetMetaOnly::Yes
                                     : GetMetaOnly::No;
    bool onlyKeys = (ctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;

    auto itr = magma->NewSeqIterator(ctx->vbid.get());

    uint64_t currSeqno = ctx->lastReadSeqno;

    for (itr->Seek(startSeqno, ctx->maxSeqno); itr->Valid(); itr->Next()) {
        Slice keySlice, metaSlice, valSlice;
        uint64_t seqno;
        itr->GetRecord(keySlice, metaSlice, valSlice, seqno);

        if (seqno < ctx->lastReadSeqno) {
            throw std::runtime_error(
                    "non-monotonic kvid:" + std::to_string(ctx->vbid.get()) +
                    " " + std::to_string(seqno) + " < " +
                    std::to_string(ctx->lastReadSeqno) + "startseqno:" +
                    std::to_string(startSeqno) + "end;" +
                    std::to_string(ctx->maxSeqno));
        }

        if (keySlice.Len() > UINT16_MAX) {
            throw std::invalid_argument(
                    "MagmaKVStore::scan: "
                    "key length " +
                    std::to_string(keySlice.Len()) + " > " +
                    std::to_string(UINT16_MAX));
        }

        std::string hex, ascii;
        if (logger->should_log(spdlog::level::debug)) {
            hexdump((char*)keySlice.Data(), keySlice.Len(), hex, ascii);
        }

        if (IsDeleted(metaSlice) &&
            ctx->docFilter == DocumentFilter::NO_DELETES) {
            logger->debug("scan SKIPPED(Deleted) key:{} {} seqno:{}",
                          ascii,
                          hex,
                          seqno);
            continue;
        }

        auto diskKey = makeDiskDocKey(keySlice);
        auto docKey = diskKey.getDocKey();

        // Determine if the key is logically deleted, if it is we skip the key
        // Note that system event keys (like create scope) are never skipped
        // here
        if (!docKey.getCollectionID().isSystem()) {
            if (ctx->docFilter !=
                DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
                if (ctx->collectionsContext.isLogicallyDeleted(docKey, seqno)) {
                    ctx->lastReadSeqno = seqno;
                    logger->debug(
                            "scan SKIPPED(Collection Deleted) key:{} {} "
                            "seqno:{}",
                            ascii,
                            hex,
                            seqno);
                    continue;
                }
            }

            CacheLookup lookup(diskKey, seqno, ctx->vbid);

            ctx->lookup->callback(lookup);
            if (ctx->lookup->getStatus() == ENGINE_KEY_EEXISTS) {
                ctx->lastReadSeqno = seqno;
                logger->debug(
                        "scan SKIPPED(ENGINE_KEY_EEXISTS) key:{} {} seqno:{}",
                        ascii,
                        hex,
                        seqno);
                continue;
            } else if (ctx->lookup->getStatus() == ENGINE_ENOMEM) {
                logger->warn("scan ENOMEM");
                return scan_again;
            }

            ctx->lastReadSeqno = seqno;
        }

        // TODO decompress
        if (ctx->valFilter != ValueFilter::KEYS_ONLY) {
        }

        logger->debug("scan key:{} {} seqno:{} deleted:{} expiry:{}",
                      ascii,
                      hex,
                      seqno,
                      IsDeleted(metaSlice),
                      GetExpiryTime(metaSlice));

        std::string valString(metaSlice.Data(), metaSlice.Len());
        if (ctx->valFilter != ValueFilter::KEYS_ONLY) {
            valString.append(valSlice.Data(), valSlice.Len());
        }
        std::unique_ptr<Item> itm =
                makeItem(ctx->vbid, diskKey, valString, isMetaOnly);

        GetValue rv(std::move(itm), ENGINE_SUCCESS, -1, onlyKeys);
        ctx->callback->callback(rv);
        auto callbackStatus = ctx->callback->getStatus();
        if (callbackStatus == ENGINE_ENOMEM) {
            logger->warn("scan ENOMEM");
            return scan_again;
        }

        currSeqno = seqno;
    }

    ctx->lastReadSeqno = currSeqno;
    return scan_success;
}

void MagmaKVStore::destroyScanContext(ScanContext* ctx) {
    if (ctx) {
        delete ctx;
    }
}

void MagmaKVStore::incrementRevision(Vbid vbid) {
    auto magmaInfo = getMagmaInfo(vbid);
    magmaInfo->fileRev++;
    if (!writeVBState(vbid, VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT)) {
        logger->warn("MagmaKVStore::incrementRevision: writeVBState failed {}",
                     vbid.to_string());
    }
}

class Snapshot;

RollbackResult MagmaKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::shared_ptr<RollbackCB> cb) {
    logger->debug("rollback {} seqno:{}", vbid, rollbackSeqno);

    Magma::FetchBuffer idxBuf;
    Magma::FetchBuffer seqBuf;
    auto cacheLookup = std::make_shared<NoLookupCallback>();
    auto fh = makeFileHandle(vbid);
    cb->setDbHeader(reinterpret_cast<void*>(fh.get()));

    auto keyCallback = [&](const Slice& keySlice,
                           const uint64_t seqno,
                           std::shared_ptr<magma::Snapshot> keySS,
                           std::shared_ptr<magma::Snapshot> seqSS) {
        auto docKey = makeDiskDocKey(keySlice);
        CacheLookup lookup(docKey, seqno, vbid);
        cacheLookup->callback(lookup);
        if (cacheLookup->getStatus() == ENGINE_KEY_EEXISTS) {
            return;
        }
        Slice meta;
        Slice value = {nullptr, 0};
        bool found;
        Status status = magma->Get(vbid.get(),
                                   keySlice,
                                   keySS,
                                   seqSS,
                                   idxBuf,
                                   seqBuf,
                                   meta,
                                   value,
                                   found);

        if (!status) {
            logger->warn(
                    "MagmaKVStore::Rollback Scan: magma::DB::Lookup error:{}, "
                    "vb:{}",
                    status.ErrorCode(),
                    vbid);
            return;
        }

        if (logger->should_log(spdlog::level::debug)) {
            std::string hex, ascii;
            hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
            logger->debug("rollback(Callback) key:{} {} seqno:{} found:{}",
                          ascii,
                          hex,
                          seqno,
                          found);
        }

        // If we don't find the item or its not the latest,
        // we are not interested.
        if (!found || GetSeqNum(meta) != seqno) {
            return;
        }

        std::string val{meta.Data(), meta.Len()};
        val.append(value.Data(), value.Len());

        auto rv = makeGetValue(vbid, docKey, val, GetMetaOnly::Yes);
        cb->callback(rv);
    };

    auto status = magma->Rollback(vbid.get(), rollbackSeqno, keyCallback);
    if (!status) {
        return RollbackResult(false, 0, 0, 0);
    }

    status = readVBState(vbid);
    if (!status) {
        return RollbackResult(false, 0, 0, 0);
    }
    auto vbstate = getVBState(vbid);

    return RollbackResult(true,
                          vbstate->highSeqno,
                          vbstate->lastSnapStart,
                          vbstate->lastSnapEnd);
}

bool MagmaKVStore::compactDB(compaction_ctx* inCtx) {
    std::chrono::steady_clock::time_point start =
            std::chrono::steady_clock::now();

    logger->debug(
            "compactDB {} purge_before_ts:{} purge_before_seq:{}"
            " drop_deletes:{} purgeSeq:{} retain_erroneous_tombstones:{}",
            inCtx->compactConfig.db_file_id,
            inCtx->compactConfig.purge_before_ts,
            inCtx->compactConfig.purge_before_seq,
            inCtx->compactConfig.drop_deletes,
            inCtx->compactConfig.purgeSeq,
            inCtx->compactConfig.retain_erroneous_tombstones);

    Vbid vbid = inCtx->compactConfig.db_file_id;

    auto droppedCollections = getDroppedCollections(vbid);
    auto ctx = std::make_shared<compaction_ctx>(inCtx->compactConfig,
                                                inCtx->max_purged_seq);
    ctx->eraserContext = std::make_unique<Collections::VB::EraserContext>(
            droppedCollections);
    ctx->curr_time = inCtx->curr_time;
    ctx->expiryCallback = inCtx->expiryCallback;
    ctx->droppedKeyCb = inCtx->droppedKeyCb;

    auto status = magma->Sync();
    if (!status) {
        logger->warn("MagmaKVStore::compactDB Sync failed. vbucket {} ",
                     ctx->compactConfig.db_file_id.to_string());
        return false;
    }

    {
        std::unique_lock<std::mutex> lock(compactionCtxMutex);
        compaction_ctxList[ctx->compactConfig.db_file_id.get()] = ctx;
    }

    // If there aren't any collections to drop, this compaction is likely
    // being called from a test because kv_engine shouldn't call compactDB
    // to compact levels, magma takes care of that already.
    if (droppedCollections.empty()) {
        status = magma->CompactKVStore(vbid.get(), Magma::StoreType::Key);
        if (!status) {
            return false;
        }
    } else {
        // Loop thru the deleted collections and run a purge scan on each.
        for (auto dc : droppedCollections) {
            std::string keyString =
                    Collections::makeCollectionIdIntoString(dc.collectionId);
            Slice keySlice{keyString};

            if (logger->should_log(spdlog::level::debug)) {
                std::string hex, ascii;
                hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
                logger->debug("PurgeKVStore {} key:{} {}", vbid, ascii, hex);
            }

            // This will find all the keys with a prefix of the collection ID
            // and drop them.
            status = magma->PurgeKVStore(vbid.get(), keySlice, keySlice, true);
            if (!status) {
                logger->warn(
                        "MagmaKVStore::compactDB PurgeKVStore {} CID:{} failed "
                        "- error:{}",
                        vbid,
                        keyString,
                        status.String());
            }

            // We've finish processing this collection.
            // Create a SystemEvent key for the collection and process it.
            auto collectionKey =
                    StoredDocKey(SystemEventFactory::makeKey(
                                         SystemEvent::Collection, keyString),
                                 CollectionID::System);

            keySlice = {reinterpret_cast<const char*>(collectionKey.data()),
                        collectionKey.size()};

            if (logger->should_log(spdlog::level::debug)) {
                std::string hex, ascii;
                hexdump(keySlice.Data(), keySlice.Len(), hex, ascii);
                logger->debug("processEndOfCollection {} key:{} {}",
                              vbid,
                              ascii,
                              hex);
            }

            auto key = makeDiskDocKey(keySlice);
            ctx->eraserContext->processEndOfCollection(key.getDocKey(),
                                                       SystemEvent::Collection);
        }
    }

    // Need to save off new vbstate and possibly collections manifest.
    // Start a new CommitBatch
    std::unique_ptr<Magma::CommitBatch> batch;
    status = magma->NewCommitBatch(vbid.get(), batch);
    if (!status) {
        logger->warn(
                "MagmaKVStore::compactDB failed creating batch for {} error:{}",
                vbid,
                status.String());
        return false;
    }

    if (ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        if (!ctx->eraserContext->empty()) {
            std::stringstream ss;
            ss << "MagmaKVStore::compactDB finalizing dropped collections, "
               << "container should be empty" << *ctx->eraserContext
               << std::endl;
            throw std::logic_error(ss.str());
        }

        // Need to ensure the 'dropped' list on disk is now gone
        std::string empty{nullptr};
        writeLocalDoc(batch.get(), droppedCollectionsKey, empty, true);
    }

    inCtx->max_purged_seq = ctx->max_purged_seq;
    auto vbstate = getVBState(vbid);
    if (vbstate) {
        vbstate->purgeSeqno = ctx->max_purged_seq;
        saveVBState(vbid, batch.get());
    }

    // Write & Sync the CommitBatch
    status = magma->ExecuteCommitBatch(std::move(batch));
    if (!status) {
        logger->warn(
                "MagmaKVStore::saveDocs: magma::ExecuteCommitBatch "
                "{} error:{}",
                vbid,
                status.String());
    } else {
        status = magma->SyncCommitBatches(true);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: magma::SyncCommitBatches {} "
                    "error:{}",
                    vbid,
                    status.String());
        }
    }

    st.compactHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

std::unique_ptr<KVFileHandle, KVFileHandleDeleter> MagmaKVStore::makeFileHandle(
        Vbid vbid) {
    std::unique_ptr<MagmaKVFileHandle, KVFileHandleDeleter> kvfh(
            new MagmaKVFileHandle(*this));
    kvfh->vbid = vbid;
    return std::move(kvfh);
}

void MagmaKVStore::saveCollectionStats(Magma::CommitBatch& batch,
                                       CollectionID cid,
                                       Collections::VB::PersistedStats stats) {
    std::string docName = "|" + cid.to_string() + "|";
    Slice keySlice = {docName.c_str(), docName.size()};

    auto encodedStats = stats.getLebEncodedStats();

    logger->debug("saveCollectionStats vb:{} cid:{} stats:{}",
                  batch.GetkvID(),
                  cid.to_string(),
                  stats.to_string());

    writeLocalDoc(&batch, keySlice, encodedStats, false);
}

void MagmaKVStore::deleteCollectionStats(Magma::CommitBatch* batch,
                                         CollectionID cid) {
    std::string docName = "|" + cid.to_string() + "|";
    Slice keySlice = {docName.c_str(), docName.size()};

    logger->debug("deleteCollectionStats vb:{} cid:{}", batch->GetkvID(), cid);

    std::string emptyValString = {nullptr, 0};
    writeLocalDoc(batch, keySlice, emptyValString, true);
}

Collections::VB::PersistedStats MagmaKVStore::getCollectionStats(
        const KVFileHandle& kvFileHandle, CollectionID cid) {
    const auto& kvfh = static_cast<const MagmaKVFileHandle&>(kvFileHandle);
    auto vbid = kvfh.vbid;

    std::string docName = "|" + cid.to_string() + "|";
    Slice keySlice = {docName.c_str(), docName.size()};
    std::string value;
    auto status = readLocalDoc(vbid, keySlice, value);
    if (!status || value.empty()) {
        return {};
    }

    auto stats = Collections::VB::PersistedStats(value.c_str(), value.size());

    logger->debug("getCollectionStats {} cid:{} stats:{}",
                  vbid,
                  cid.to_string(),
                  stats.to_string());
    return stats;
}

void MagmaKVStore::EncodeLocalDoc(const Vbid vbid,
                                  const std::string& value,
                                  const bool isDelete,
                                  std::string& retString) {
    uint64_t seqno;
    magma->GetMaxSeqno(vbid.get(), seqno);
    auto docMeta = magmakv::MetaData(isDelete,
                                     sizeof(magmakv::MetaData) + value.size(),
                                     ep_real_time(),
                                     seqno,
                                     vbid);
    retString = {reinterpret_cast<char*>(&docMeta), sizeof(magmakv::MetaData)};
    retString.append(value.data(), value.size());
}

void MagmaKVStore::DecodeLocalDoc(const Slice& valSlice,
                                  std::string& value,
                                  bool& isDelete) {
    isDelete = IsDeleted(valSlice);
    value = {valSlice.Data() + sizeof(magmakv::MetaData),
             valSlice.Len() - sizeof(magmakv::MetaData)};
}

Status MagmaKVStore::readLocalDoc(Vbid vbid,
                                  const Slice& keySlice,
                                  std::string& valString) {
    Status status;
    Slice valSlice = {nullptr, 0};
    Magma::FetchBuffer valBuf;
    bool found;
    bool isDelete;

    status = magma->GetLocal(vbid.get(), keySlice, valBuf, valSlice, found);
    if (!status) {
        logger->critical("MagmaKVStore::readLocalDoc({} key:{}) status:{}",
                         vbid,
                         const_cast<Slice&>(keySlice).ToString(),
                         status.String());
        return status;
    }

    if (!found) {
        // If not found, return empty value
        valString = "";
        logger->warn("MagmaKVStore::readLocalDoc({} key:{}) not found",
                     vbid,
                     const_cast<Slice&>(keySlice).ToString());
    } else {
        DecodeLocalDoc(valSlice, valString, isDelete);
        // If deleted, return empty value
        if (isDelete) {
            valString = "";
            logger->warn("MagmaKVStore::readLocalDoc({} key:{}) deleted",
                         vbid,
                         const_cast<Slice&>(keySlice).ToString());
        } else {
            logger->debug("MagmaKVStore::readLocalDoc({} key:{})",
                          vbid,
                          const_cast<Slice&>(keySlice).ToString());
        }
    }

    return Status::OK();
}

Status MagmaKVStore::writeLocalDoc(Magma::CommitBatch* batch,
                                   const Slice& keySlice,
                                   std::string& valBuf,
                                   bool deleted) {
    std::string valString;

    EncodeLocalDoc(Vbid(batch->GetkvID()), valBuf, deleted, valString);

    // Store in localDB
    Slice valSlice = {valString.c_str(), valString.size()};
    logger->debug("MagmaKVStore::writeLocalDoc(vbid:{} key:{})",
                  batch->GetkvID(),
                  const_cast<Slice&>(keySlice).ToString());
    return batch->SetLocal(keySlice, valSlice);
}

Collections::KVStore::Manifest MagmaKVStore::getCollectionsManifest(Vbid vbid) {
    Collections::KVStore::Manifest rv{Collections::KVStore::Manifest::Empty{}};
    Status status;
    std::string manifest;

    status = readLocalDoc(vbid, manifestKey, manifest);
    if (!status) {
        return Collections::KVStore::Manifest{
                Collections::KVStore::Manifest::Default{}};
    } else if (!manifest.empty()) {
        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::CommittedManifest>(
                        reinterpret_cast<const uint8_t*>(manifest.c_str()));
        rv.manifestUid = fbData->uid();
    }

    std::string collections;
    status = readLocalDoc(vbid, openCollectionsKey, collections);
    if (!status) {
        return Collections::KVStore::Manifest{
                Collections::KVStore::Manifest::Default{}};
    } else if (collections.empty()) {
        rv.collections.push_back(
                {0,
                 {ScopeID::Default,
                  CollectionID::Default,
                  Collections::DefaultCollectionIdentifier.data(),
                  {}}});
    } else {
        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                        reinterpret_cast<const uint8_t*>(collections.c_str()));

        for (const auto& entry : *fbData->entries()) {
            cb::ExpiryLimit maxTtl;
            if (entry->ttlValid()) {
                maxTtl = std::chrono::seconds(entry->maxTtl());
            }

            rv.collections.push_back(
                    {entry->startSeqno(),
                     Collections::CollectionMetaData{entry->scopeId(),
                                                     entry->collectionId(),
                                                     entry->name()->str(),
                                                     maxTtl}});
        }
    }

    std::string scopes;
    status = readLocalDoc(vbid, openScopesKey, scopes);
    if (!status) {
        return Collections::KVStore::Manifest{
                Collections::KVStore::Manifest::Default{}};
    } else if (scopes.empty()) {
        // Nothing on disk - the default scope is assumed
        rv.scopes.push_back(ScopeID::Default);
    } else {
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                reinterpret_cast<const uint8_t*>(scopes.c_str()));
        for (const auto& entry : *fbData->entries()) {
            rv.scopes.push_back(entry);
        }
    }

    rv.droppedCollectionsExist = getDroppedCollectionCount(vbid) > 0;
    return rv;
}

std::vector<Collections::KVStore::DroppedCollection>
MagmaKVStore::getDroppedCollections(Vbid vbid) {
    std::string droppedCollections;
    std::vector<Collections::KVStore::DroppedCollection> rv;

    auto status = readLocalDoc(vbid, droppedCollectionsKey, droppedCollections);
    if (!status || droppedCollections.empty()) {
        return {};
    }

    auto fbData =
            flatbuffers::GetRoot<Collections::KVStore::DroppedCollections>(
                    reinterpret_cast<const uint8_t*>(
                            droppedCollections.c_str()));
    for (const auto& entry : *fbData->entries()) {
        rv.push_back({entry->startSeqno(),
                      entry->endSeqno(),
                      entry->collectionId()});
    }
    return rv;
}

size_t MagmaKVStore::getDroppedCollectionCount(Vbid vbid) {
    std::string droppedCollections;
    auto status = readLocalDoc(vbid, droppedCollectionsKey, droppedCollections);
    if (!status || droppedCollections.empty()) {
        return {};
    }

    auto fbData =
            flatbuffers::GetRoot<Collections::KVStore::DroppedCollections>(
                    reinterpret_cast<const uint8_t*>(
                            droppedCollections.c_str()));
    return fbData->entries()->size();
}

Status MagmaKVStore::updateCollectionsMeta(
        Vbid vbid,
        Magma::CommitBatch* batch,
        Collections::VB::Flush& collectionsFlush) {
    // Write back, no read required
    // This code is the same as updateManifestUid in couch-kvstore.cc
    flatbuffers::FlatBufferBuilder builder;
    auto toWrite = Collections::KVStore::CreateCommittedManifest(
            builder, collectionsMeta.manifestUid);
    builder.Finish(toWrite);
    std::string manifestMeta{
            reinterpret_cast<const char*>(builder.GetBufferPointer()),
            builder.GetSize()};
    Status status = writeLocalDoc(batch, manifestKey, manifestMeta, false);
    if (!status) {
        return status;
    }

    // If the updateOpenCollections reads the dropped collections, it can pass
    // them via this optional to updateDroppedCollections, thus we only read
    // the dropped list once per update.
    boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
            dropped;

    if (!collectionsMeta.collections.empty() ||
        !collectionsMeta.droppedCollections.empty()) {
        auto rv = updateOpenCollections(vbid, batch);
        if (!rv.first) {
            return rv.first;
        }
        // move the dropped list read here out so it doesn't need reading again
        dropped = std::move(rv.second);
    }

    if (!collectionsMeta.droppedCollections.empty()) {
        status = updateDroppedCollections(vbid, batch, dropped);
        if (!status) {
            return status;
        }
        collectionsFlush.setNeedsPurge();
    }

    if (!collectionsMeta.scopes.empty() ||
        !collectionsMeta.droppedScopes.empty()) {
        status = updateScopes(vbid, batch);
        if (!status) {
            return status;
        }
    }

    collectionsMeta.clear();
    return Status::OK();
}

std::pair<Status, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::updateOpenCollections(Vbid vbid, Magma::CommitBatch* batch) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Collection>>
            openCollections;

    // Get the dropped collections to protect against duplicate CIDs
    auto droppedCollections = getDroppedCollections(vbid);

    for (const auto& event : collectionsMeta.collections) {
        const auto& meta = event.metaData;
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                event.startSeqno,
                meta.sid,
                meta.cid,
                meta.maxTtl.is_initialized(),
                meta.maxTtl.is_initialized() ? meta.maxTtl.get().count() : 0,
                builder.CreateString(meta.name.data(), meta.name.size()));
        openCollections.push_back(newEntry);

        // Validate we are not adding a dropped collection
        auto itr = std::find_if(
                droppedCollections.begin(),
                droppedCollections.end(),
                [&meta](const Collections::KVStore::DroppedCollection&
                                dropped) {
                    return dropped.collectionId == meta.cid;
                });
        if (itr != droppedCollections.end()) {
            // we have found the created collection in the drop list, not good
            throw std::logic_error(
                    "MagmaKVStore::updateOpenCollections found a new "
                    "collection in dropped list, cid:" +
                    meta.cid.to_string());
        }
    }

    // And 'merge' with the data we read
    std::string buffer;
    Status status = readLocalDoc(vbid, openCollectionsKey, buffer);
    if (!status || buffer.empty()) {
        // Nothing on disk - assume the default collection lives
        auto newEntry = Collections::KVStore::CreateCollection(
                builder,
                0,
                ScopeID::Default,
                CollectionID::Default,
                false /* ttl invalid*/,
                0,
                builder.CreateString(
                        Collections::DefaultCollectionIdentifier.data()));
        openCollections.push_back(newEntry);
    } else {
        auto fbData =
                flatbuffers::GetRoot<Collections::KVStore::OpenCollections>(
                        reinterpret_cast<const uint8_t*>(buffer.c_str()));
        for (const auto& entry : *fbData->entries()) {
            auto p = [entry](const Collections::KVStore::DroppedCollection& c) {
                return c.collectionId == entry->collectionId();
            };
            auto result =
                    std::find_if(collectionsMeta.droppedCollections.begin(),
                                 collectionsMeta.droppedCollections.end(),
                                 p);

            // If not found in dropped collections add to output
            if (result == collectionsMeta.droppedCollections.end()) {
                auto newEntry = Collections::KVStore::CreateCollection(
                        builder,
                        entry->startSeqno(),
                        entry->scopeId(),
                        entry->collectionId(),
                        entry->ttlValid(),
                        entry->maxTtl(),
                        builder.CreateString(entry->name()));
                openCollections.push_back(newEntry);
            } else {
                // Here we maintain the startSeqno of the dropped collection
                result->startSeqno = entry->startSeqno();
            }
        }
    }

    auto collectionsVector = builder.CreateVector(openCollections);
    auto toWrite = Collections::KVStore::CreateOpenCollections(
            builder, collectionsVector);

    builder.Finish(toWrite);

    // write back
    std::string valString{
            reinterpret_cast<const char*>(builder.GetBufferPointer()),
            builder.GetSize()};
    return {writeLocalDoc(batch, openCollectionsKey, valString, false),
            droppedCollections};
}

Status MagmaKVStore::updateDroppedCollections(
        Vbid vbid,
        Magma::CommitBatch* batch,
        boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
                dropped) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<Collections::KVStore::Dropped>>
            droppedCollections;
    for (const auto& dropped : collectionsMeta.droppedCollections) {
        auto newEntry =
                Collections::KVStore::CreateDropped(builder,
                                                    dropped.startSeqno,
                                                    dropped.endSeqno,
                                                    dropped.collectionId);
        droppedCollections.push_back(newEntry);

        // Delete the 'stats' document for the collection
        deleteCollectionStats(batch, dropped.collectionId);
    }

    // If the input 'dropped' is not initialised we must read the dropped
    // collection data
    if (!dropped.is_initialized()) {
        dropped = getDroppedCollections(vbid);
    }

    for (const auto& entry : dropped.get()) {
        auto newEntry = Collections::KVStore::CreateDropped(
                builder, entry.startSeqno, entry.endSeqno, entry.collectionId);
        droppedCollections.push_back(newEntry);
    }

    auto vector = builder.CreateVector(droppedCollections);
    auto final =
            Collections::KVStore::CreateDroppedCollections(builder, vector);
    builder.Finish(final);

    // write back
    std::string buffer{
            reinterpret_cast<const char*>(builder.GetBufferPointer()),
            builder.GetSize()};
    return writeLocalDoc(batch, droppedCollectionsKey, buffer, false);
}

Status MagmaKVStore::updateScopes(Vbid vbid, Magma::CommitBatch* batch) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<ScopeIDType> openScopes;
    for (const auto& sid : collectionsMeta.scopes) {
        openScopes.push_back(sid);
    }

    // And 'merge' with the data we read (remove any dropped)
    std::string buffer;
    auto status = readLocalDoc(vbid, openScopesKey, buffer);
    if (!status || buffer.empty()) {
        // Nothing on disk, the default scope is assumed to exist
        openScopes.push_back(ScopeID::Default);
    } else {
        auto fbData = flatbuffers::GetRoot<Collections::KVStore::Scopes>(
                reinterpret_cast<const uint8_t*>(buffer.c_str()));

        for (const auto& sid : *fbData->entries()) {
            auto result = std::find(collectionsMeta.droppedScopes.begin(),
                                    collectionsMeta.droppedScopes.end(),
                                    sid);

            // If not found in dropped scopes add to output
            if (result == collectionsMeta.droppedScopes.end()) {
                openScopes.push_back(sid);
            }
        }
    }

    auto vector = builder.CreateVector(openScopes);
    auto final = Collections::KVStore::CreateScopes(builder, vector);
    builder.Finish(final);

    // write back
    buffer = {reinterpret_cast<const char*>(builder.GetBufferPointer()),
              builder.GetSize()};
    return writeLocalDoc(batch, openScopesKey, buffer, false);
}

void MagmaKVStore::addStats(const AddStatFn& add_stat,
                       const void* c,
                       const std::string& args) {
    std::string statName;
    std::string kvidArg = args.substr(1, args.length() - 1);
    if (kvidArg.length() > 0) {
        auto kvid = atoi(kvidArg.c_str());
        if (kvid >= 0 && kvid < UINT16_MAX) {
            statName = "kvstore";
            add_casted_stat(statName.c_str(), magma->GetKVStoreStats(kvid).dump(), add_stat, c);
        }
    } else {
        statName = "magma";
        Magma::MagmaStats stats;
        magma->GetStats(stats);
        add_casted_stat(statName.c_str(), stats.JSON().dump(), add_stat, c);
    }
}

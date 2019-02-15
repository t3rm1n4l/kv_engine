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

#pragma once

#include "collections/collection_persisted_stats.h"
#include "kvstore.h"
#include "kvstore_priv.h"
#include "libmagma/magma.h"
#include "vbucket_bgfetch_item.h"
#include <mcbp/protocol/request.h>
#include <platform/dirutils.h>
#include <map>
#include <string>
#include <vector>

class MagmaRequest;
class MagmaKVStoreConfig;
class KVMagma;

struct MagmaInfo {
    uint64_t docCount = 0;
    uint64_t persistedDeletes = 0;
    uint64_t fileRev = 0;
};

struct kvstats_ctx;

/**
 * A persistence store based on magma.
 */
class MagmaKVStore : public KVStore {
public:
    MagmaKVStore(MagmaKVStoreConfig& config);
    ~MagmaKVStore();

    void operator=(MagmaKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(Vbid vbid) override;

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx) override;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(Collections::VB::Flush& collectionsFlush) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() override;

    /**
     * Get the properties of the underlying storage.
     */
    StorageProperties getStorageProperties() override;

    /**
     * Adds a request to a queue for batch processing at commit()
     */
    void set(const Item& item, SetCallback cb) override;

    /**
     * Get an item from the kv store.
     */
    GetValue get(const DiskDocKey& key,
                 Vbid vb,
                 bool fetchDelete = false) override;

    GetValue getWithHeader(void* dbHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    /**
     * Get multiple items if supported by the kv store
     */
    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Delete an item from the kv store.
     *
     * @param item The item to delete
     * @param cb Pointer to a callback object which will be invoked when the
     *           del() has been persisted to disk.
     */
    void del(const Item& itm, DeleteCallback cb) override;

    /**
     * Delete a given vbucket database instance from underlying storage
     *
     * @param vbucket vbucket id
     * @param fileRev the revision of the file to delete
     */
    void delVBucket(Vbid vbucket, uint64_t fileRev) override;

    /**
     * Get a list of all persisted vbuckets (with their states).
     */
    std::vector<vbucket_state*> listPersistedVbuckets(void) override;

    /**
     * Get a list of all persisted engine and DCP stats. This API is mainly
     * invoked during warmup to get the engine stats from the previous session.
     *
     * @param stats map instance where the engine stats from the previous
     * session is stored.
     */
    // TODO
    void getPersistedStats(std::map<std::string, std::string>& stats) override {
    }

    /**
     * Snapshot vbucket state
     * @param vbucketId id of the vbucket that needs to be snapshotted
     * @param vbstate   state of the vbucket
     * @param cb        stats callback
     * @param options   options for persisting the state
     */
    bool snapshotVBucket(Vbid vbucketId,
                         const vbucket_state& vbstate,
                         VBStatePersist options) override;

    // Not used
    uint16_t getNumVbsPerFile() override {
        return 1024;
    }

    /**
     * Compact a database file.
     *
     * For Magma, there are 2 kinds of compaction, implicit and explicit.
     * There are 2 kinds of implicit compactions:
     * 1. Level compaction - controlled by the LSMs
     * 2. Purge compaction - this compaction purges expiried items and
     * tombstones which are kept track of in the seq Index by the LSM. The
     * frequency of purge compactions is configurable with default set 1 once
     * per hour for each kvstore. Purge compactions are inexpensive (require no
     * IO) unless there is purging work to do. Implicit compactions run
     * concurrently with the bg flusher thread.
     *
     * There are 2 kinds of explicit compactions:
     * An explicit compaction comes thru the bg flusher thread via a call to
     * compactDB.
     * 1. Dropped collection - when a collection is dropped, compactDB will run
     *    purge scans for the dropped collection to make sure all keys for that
     *    collection have been deleted.
     * 2. When compactDB is run and there are no dropped collections, a full
     *    compaction scan is run. This happens mostly for tests.
     * Explicit compactions block the bg flusher thread until completion.
     */
    bool compactDB(compaction_ctx* inCtx) override;

    // Not used
    Vbid getDBFileId(const cb::mcbp::Request& req) override {
        Vbid vbid{0};
        return vbid;
    }

    /**
     * Return the vbucket_state for a given kvstore from the cache
     */
    vbucket_state* getVBucketState(Vbid vbucketId) override {
        return cachedVBStates[vbucketId.get()].get();
    }

    /**
     * Return the # of persisted deletes as maintained in magmaInfo.
     * Note: This isn't accurate and is mainly used for testing
     */
    size_t getNumPersistedDeletes(Vbid vbid) override {
        auto magmaInfo = getMagmaInfo(vbid);
        return size_t(magmaInfo->persistedDeletes);
    }

    /**
     * Not supported by Magma.
     * This is mainly used to determine when compaction needs to run
     * but compaction is controlled by magma.
     */
    DBFileInfo getDbFileInfo(Vbid vbid) override {
        DBFileInfo vbinfo;
        return vbinfo;
    }

    /**
     * Not supported by Magma.
     */
    DBFileInfo getAggrDbFileInfo() override {
        DBFileInfo vbinfo;
        return vbinfo;
    }

    /**
     * This method will return the total number of items in the vbucket
     *
     * vbid - vbucket id
     */
    size_t getItemCount(Vbid vbid) override {
        auto magmaInfo = getMagmaInfo(vbid);
        return size_t(magmaInfo->docCount);
    }

    /**
     * Rollback the specified vBucket to the state it had at rollbackseqno.
     *
     * On success, the vBucket should have discarded *at least* back to the
     * specified rollbackseqno; if necessary it is valid to rollback further.
     * A minimal implementation is permitted to rollback to zero.
     *
     * @param vbid VBucket to rollback
     * @param rollbackseqno Sequence number to rollback to (minimum).
     * @param cb For each mutation which has been rolled back (i.e. from the
     * selected rollback point to the latest); invoke this callback with the Key
     * of the now-discarded update. Callers can use this to undo the effect of
     * the discarded updates on their in-memory view.
     * @return success==true and details of the sequence numbers after rollback
     * if rollback succeeded; else false.
     */
    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override;

    // Not used in magma.
    void pendingTasks() override {
    }

    // Not used in magma.
    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DiskDocKey&>> cb) override {
        // Unused
        return ENGINE_SUCCESS;
    }

    /**
     * Create a KVStore Scan Context with the given options. On success,
     * returns a pointer to the ScanContext. The caller can then call scan()
     * to execute the scan. The context should be deleted by the caller using
     * destroyScanContext() when finished with.
     *
     * The caller specifies two callback objects - GetValue and CacheLookup:
     *
     * 1. GetValue callback is invoked for each object loaded from disk, for
     *    the caller to process that item.
     * 2. CacheLookup callback an an optimization to avoid loading data from
     *    disk for already-resident items - it is invoked _before_ loading the
     *    item's value from disk, to give ep-engine's in-memory cache the
     *    opportunity to fulfill the item (assuming the item is in memory).
     *    If this callback has status ENGINE_KEY_EEXISTS then the document is
     *    considered to have been handled purely from memory and the GetValue
     *    callback is skipped.
     *    If this callback has status ENGINE_SUCCESS then it wasn't fulfilled
     *    from memory, and will instead be loaded from disk and GetValue
     *    callback invoked.
     *
     * @param cb GetValue callback
     * @param cl Cache lookup callback
     * If the ScanContext cannot be created, returns null.
     */
    ScanContext* initScanContext(
            std::shared_ptr<StatusCallback<GetValue>> cb,
            std::shared_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

    /**
     * Increment the revision number of the vbucket.
     * @param vbid ID of the vbucket to change.
     */
    void incrementRevision(Vbid vbid) override;

    // Not used in magma
    uint64_t prepareToDelete(Vbid vbid) override {
        return 0;
    }

    /**
     * Encode a document being stored in the local db by prepending the
     * MetaData to the value
     */
    void EncodeLocalDoc(const Vbid vbid,
                        const std::string& value,
                        const bool isDelete,
                        std::string& retValue);
    /**
     * Decode a document being stored in the local db by extricating the
     * MetaData from the value
     */
    void DecodeLocalDoc(const Slice& valSlice,
                        std::string& value,
                        bool& isDelete);

    /**
     * Initialize the VBState
     */
    vbucket_state* initVBState(const Vbid vbid);

    /**
     * Get the vbstate from the cache
     */
    vbucket_state* getVBState(const Vbid vbid);

    /**
     * Encode the vbstate and magmaInfo into jason document
     */
    void encodeVBState(const vbucket_state* vbstate,
                       const MagmaInfo* magmaInfo,
                       nlohmann::json& vbstateJSON);
    /**
     * Dencode the vbstate and magmaInfo from Slice to structures
     */
    Status decodeVBState(const Vbid vbid, const std::string& valString);

    /**
     * Read the vbstate from the local db and decode to structure
     */
    Status readVBState(const Vbid vbid);

    /**
     * Write the encoded vbstate to the local db.
     * This creates its own CommitBatch.
     */
    bool writeVBState(Vbid vbid, VBStatePersist options);

    /**
     * Save the vbstate to local db as part of existing CommitBatch
     */
    void saveVBState(Vbid vbid, Magma::CommitBatch* batch);

    /**
     * Get the MagmaInfo from the cache
     */
    MagmaInfo* getMagmaInfo(const Vbid vbid);

    size_t getNumShards() const;

    class MagmaKVFileHandle : public ::KVFileHandle {
    public:
        MagmaKVFileHandle(MagmaKVStore& kvstore) : ::KVFileHandle(kvstore) {
        }
        Vbid vbid;
    };

    std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) override;

    void freeFileHandle(KVFileHandle* kvFileHandle) const override {
        delete kvFileHandle;
    }

    void saveCollectionStats(Magma::CommitBatch& batch,
                             CollectionID cid,
                             Collections::VB::PersistedStats stats);
    void deleteCollectionStats(Magma::CommitBatch* batch, CollectionID cid);
    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) override;

    Status readLocalDoc(Vbid vbid, const Slice& keySlice, std::string& valBuf);
    Status writeLocalDoc(Magma::CommitBatch* batch,
                         const Slice& keySlice,
                         std::string& valBuf,
                         bool deleted);

    Collections::KVStore::Manifest getCollectionsManifest(Vbid vbid) override;
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override;
    size_t getDroppedCollectionCount(Vbid vbid);
    Status updateCollectionsMeta(Vbid vbid,
                                 Magma::CommitBatch* batch,
                                 Collections::VB::Flush& collectionsFlush);
    std::pair<Status, std::vector<Collections::KVStore::DroppedCollection>>
    updateOpenCollections(Vbid vbid, Magma::CommitBatch* batch);
    Status updateDroppedCollections(
            Vbid vbid,
            Magma::CommitBatch* batch,
            boost::optional<
                    std::vector<Collections::KVStore::DroppedCollection>>
                    dropped);
    Status updateScopes(Vbid vbid, Magma::CommitBatch* batch);

private:
    // Magma instance for a shard.
    std::unique_ptr<Magma> magma;

    /**
     * Container for pending Magma requests.
     *
     * Using deque as as the expansion behaviour is less aggressive compared to
     * std::vector (MagmaRequest objects are ~176 bytes in size).
     */
    using PendingRequestQueue = std::deque<MagmaRequest>;

    /*
     * The DB for each VBucket is created in a separated subfolder of
     * `configuration.getDBName()`. This function returns the path of the DB
     * subfolder for the given `vbid`.
     *
     * @param vbid vbucket id for the vbucket DB subfolder to return
     */
    std::string getVBDBSubdir(Vbid vbid);

    std::unique_ptr<Item> makeItem(Vbid vb,
                                   const DiskDocKey& key,
                                   const std::string& value,
                                   GetMetaOnly getMetaOnly);

    GetValue makeGetValue(Vbid vb,
                          const DiskDocKey& key,
                          const std::string& value,
                          GetMetaOnly getMetaOnly = GetMetaOnly::No);

    int saveDocs(Vbid vbid,
                 Collections::VB::Flush& collectionsFlush,
                 kvstats_ctx& kvctx);

    void commitCallback(
            int status,
            kvstats_ctx& kvctx,
            Vbid vbid);

    int64_t readHighSeqnoFromDisk(Vbid vbid);

    std::string getVbstateKey();

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    // unique_ptr for pimpl.
    std::unique_ptr<PendingRequestQueue> pendingReqs;

    // This variable is used to verify that the KVStore API is used correctly
    // when Magma is used as store. "Correctly" means that the caller must
    // use the API in the following way:
    //      - begin() x1
    //      - set() / del() xN
    //      - commit()
    bool in_transaction;
    std::unique_ptr<TransactionContext> transactionCtx;
    std::string magmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    std::shared_ptr<BucketLogger> logger;
    std::vector<std::unique_ptr<MagmaInfo>> cachedMagmaInfo;
    bool batchCommitPoint = false;
    bool getBatchCommitPoint() {
        return batchCommitPoint;
    };
    void setBatchCommitPoint(bool bcp) {
        batchCommitPoint = bcp;
    };

    std::vector<std::shared_ptr<compaction_ctx>> compaction_ctxList;
    std::mutex compactionCtxMutex;

    friend class MagmaCompactionCB;
};

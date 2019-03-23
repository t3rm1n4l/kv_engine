/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include "ep_test_apis.h"
#include "ep_testsuite_common.h"

#include <mcbp/protocol/framebuilder.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>

#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>
#include <thread>

#include "mock/mock_dcp.h"

void RequestDeleter::operator()(cb::mcbp::Request* request) {
    delete[] reinterpret_cast<uint8_t*>(request);
}

template<typename T> class HistogramStats;

// Due to the limitations of the add_stats callback (essentially we cannot pass
// a context into it) we instead have a single, global `vals` map. The
// vals_mutex is to ensure serialised modifications to this data structure.
std::mutex vals_mutex;
statistic_map vals;

// get_stat and get_histo_stat can only be called one at a time as they use
// the three global variables (requested_stat_name, actual_stat_value and
// histogram_stat_int_value).  Therefore the two functions need to acquire a
// lock and keep it for the whole function duration.

// The requested_stat_name and actual_stat_value are used in an optimized
// add_stats callback (add_individual_stat) which checks for one stat
// (and hence doesn't have to keep a map of all of them).
struct {
    std::mutex mutex;
    std::string requested_stat_name;
    std::string actual_stat_value;
    /* HistogramStats<T>* is supported C++14 onwards.
     * Until then use a separate ptr for each type.
     */
    HistogramStats<uint64_t>* histogram_stat_int_value;
} get_stat_context;

bool dump_stats = false;
std::atomic<cb::mcbp::Status> last_status(cb::mcbp::Status::Success);
std::string last_key;
std::string last_body;
std::string last_ext;
std::atomic<uint64_t> last_cas(0);
std::atomic<uint8_t> last_datatype(0x00);
ItemMetaData last_meta;
std::atomic<uint64_t> last_uuid(0);
std::atomic<uint64_t> last_seqno(0);

/* HistogramBinStats is used to hold a histogram bin object a histogram stat.
   This is a class used to hold already computed stats. Hence we do not expect
   any change once a bin object is created */
template<typename T>
class HistogramBinStats {
public:
    HistogramBinStats(const T& s, const T& e, uint64_t count)
        : start_(s), end_(e), count_(count) { }

    T start() const {
        return start_;
    }

    T end() const {
        return end_;
    }

    uint64_t count() const {
        return count_;
    }

private:
    T start_;
    T end_;
    uint64_t count_;
};


/* HistogramStats is used to hold necessary info from a histogram stat.
   Since this class used to hold already computed stats, only write apis to add
   new bins is implemented */
template<typename T>
class HistogramStats {
public:
    HistogramStats() : total_count(0) {}

    /* Add a new bin */
    void add_bin(const T& start, const T& end, uint64_t count) {
        bins.push_back(HistogramBinStats<T>(start, end, count));
        total_count += count;
    }

    /* Num of bins in the histogram */
    size_t num_bins() const {
        return bins.size();
    }

    uint64_t total() const {
        return total_count;
    }

    /* Add a bin iterator when needed */
private:
    /* List of all the bins in the histogram stats */
    std::list<HistogramBinStats<T>> bins;
    /* Total number of samples across all histogram bins */
    uint64_t total_count;
};

static void get_histo_stat(EngineIface* h,
                           const char* statname,
                           const char* statkey);

void encodeExt(char* buffer, uint32_t val, size_t offset = 0);
void encodeWithMetaExt(char *buffer, ItemMetaData *meta);

void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    std::this_thread::sleep_for(std::chrono::microseconds(*sleepTime));
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

bool add_response(const void* key,
                  uint16_t keylen,
                  const void* ext,
                  uint8_t extlen,
                  const void* body,
                  uint32_t bodylen,
                  uint8_t datatype,
                  cb::mcbp::Status status,
                  uint64_t cas,
                  const void* cookie) {
    (void)cookie;
    static std::mutex m;
    std::lock_guard<std::mutex> lg(m);
    last_status.store(status);
    last_body.assign(static_cast<const char*>(body), bodylen);
    last_ext.assign(static_cast<const char*>(ext), extlen);
    last_key.assign(static_cast<const char*>(key), keylen);
    last_cas.store(cas);
    last_datatype.store(datatype);
    return true;
}

bool add_response_set_del_meta(const void* key,
                               uint16_t keylen,
                               const void* ext,
                               uint8_t extlen,
                               const void* body,
                               uint32_t bodylen,
                               uint8_t datatype,
                               cb::mcbp::Status status,
                               uint64_t cas,
                               const void* cookie) {
    (void)cookie;
    const auto* ext_bytes = reinterpret_cast<const uint8_t*>(ext);
    if (ext && extlen > 0) {
        uint64_t vb_uuid;
        uint64_t seqno;
        memcpy(&vb_uuid, ext_bytes, 8);
        memcpy(&seqno, ext_bytes + 8, 8);
        last_uuid.store(ntohll(vb_uuid));
        last_seqno.store(ntohll(seqno));
    }

    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

bool add_response_ret_meta(const void* key,
                           uint16_t keylen,
                           const void* ext,
                           uint8_t extlen,
                           const void* body,
                           uint32_t bodylen,
                           uint8_t datatype,
                           cb::mcbp::Status status,
                           uint64_t cas,
                           const void* cookie) {
    (void)cookie;
    const auto* ext_bytes = reinterpret_cast<const uint8_t*>(ext);
    if (ext && extlen == 16) {
        memcpy(&last_meta.flags, ext_bytes, 4);
        memcpy(&last_meta.exptime, ext_bytes + 4, 4);
        last_meta.exptime = ntohl(last_meta.exptime);
        uint64_t revId = 0;
        memcpy(&revId, ext_bytes + 8, 8);
        last_meta.revSeqno = ntohll(revId);
        last_meta.cas = cas;
    }
    return add_response(key, keylen, ext, extlen, body, bodylen, datatype,
                        status, cas, cookie);
}

void add_stats(const char* key,
               const uint16_t klen,
               const char* val,
               const uint32_t vlen,
               gsl::not_null<const void*>) {
    std::string k(key, klen);
    std::string v(val, vlen);

    if (dump_stats) {
        std::cout << "stat[" << k << "] = " << v << std::endl;
    }

    std::lock_guard<std::mutex> lh(vals_mutex);
    vals[k] = v;
}

/* Callback passed to engine interface `get_stats`, used by get_int_stat and
 * friends to lookup a specific stat. If `key` matches the requested key name,
 * then record its value in actual_stat_value.
 */
void add_individual_stat(const char* key,
                         const uint16_t klen,
                         const char* val,
                         const uint32_t vlen,
                         gsl::not_null<const void*>) {
    if (get_stat_context.actual_stat_value.empty() &&
            get_stat_context.requested_stat_name.compare(
                    0, get_stat_context.requested_stat_name.size(),
                    key, klen) == 0) {
        get_stat_context.actual_stat_value = std::string(val, vlen);
    }
}

void add_individual_histo_stat(const char* key,
                               const uint16_t klen,
                               const char* val,
                               const uint32_t vlen,
                               gsl::not_null<const void*> cookie) {
    /* Convert key to string */
    std::string key_str(key, klen);
    size_t pos1 = key_str.find(get_stat_context.requested_stat_name);
    if (pos1 != std::string::npos)
    {
        get_stat_context.actual_stat_value.append(val, vlen);
        /* Parse start and end from the key.
           Key is in the format task_name_START,END (backfill_tasks_20,100) */
        pos1 += get_stat_context.requested_stat_name.length();
        /* Find ',' to move to end of bin_start */
        size_t pos2 = key_str.find(',', pos1);
        if ((std::string::npos == pos2) || (pos1 >= pos2)) {
            throw std::invalid_argument("Malformed histogram stat: " + key_str);
        }
        auto start = std::stoull(std::string(key_str, pos1, pos2));

        /* Move next to ',' for starting character of bin_end */
        pos1 = pos2 + 1;
        /* key_str ends with bin_end */
        pos2 = key_str.length();
        if (pos1 >= pos2) {
            throw std::invalid_argument("Malformed histogram stat: " + key_str);
        }
        auto end = std::stoull(std::string(key_str, pos1, pos2));
        get_stat_context.histogram_stat_int_value->add_bin(start, end,
                                                           std::stoull(val));
    }
}

void encodeExt(char* buffer, uint32_t val, size_t offset) {
    val = htonl(val);
    memcpy(buffer + offset, (char*)&val, sizeof(val));
}

void encodeWithMetaExt(char* buffer,
                       uint64_t cas,
                       uint64_t revSeqno,
                       uint32_t flags,
                       uint32_t exp) {
    memcpy(buffer, (char*)&flags, sizeof(flags));
    memcpy(buffer + 4, (char*)&exp, sizeof(exp));
    memcpy(buffer + 8, (char*)&revSeqno, sizeof(revSeqno));
    memcpy(buffer + 16, (char*)&cas, sizeof(cas));
}

void encodeWithMetaExt(char* buffer, RawItemMetaData* meta) {
    uint32_t flags = meta->flags;
    uint32_t exp = htonl(meta->exptime);
    uint64_t seqno = htonll(meta->revSeqno);
    uint64_t cas = htonll(meta->cas);
    encodeWithMetaExt(buffer, cas, seqno, flags, exp);
}

void encodeWithMetaExt(char* buffer, ItemMetaData* meta) {
    uint32_t flags = meta->flags;
    uint32_t exp = htonl(meta->exptime);
    uint64_t seqno = htonll(meta->revSeqno);
    uint64_t cas = htonll(meta->cas);
    encodeWithMetaExt(buffer, cas, seqno, flags, exp);
}

unique_request_ptr createPacket(cb::mcbp::ClientOpcode opcode,
                                Vbid vbid,
                                uint64_t cas,
                                cb::const_char_buffer ext,
                                cb::const_char_buffer key,
                                cb::const_char_buffer val,
                                uint8_t datatype,
                                cb::const_char_buffer meta) {
    using namespace cb::mcbp;

    const auto total = sizeof(cb::mcbp::Request) + ext.size() + key.size() +
                       val.size() + meta.size();
    std::unique_ptr<uint8_t[]> memory(new uint8_t[total]);
    RequestBuilder builder({memory.get(), total});

    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(opcode);
    builder.setVBucket(vbid);
    builder.setCas(cas);
    builder.setDatatype(cb::mcbp::Datatype(datatype));
    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(ext.data()), ext.size()});
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    if (meta.empty()) {
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(val.data()), val.size()});
    } else {
        std::vector<uint8_t> backing;
        std::copy(val.begin(), val.end(), std::back_inserter(backing));
        std::copy(meta.begin(), meta.end(), std::back_inserter(backing));
        builder.setValue({backing.data(), backing.size()});
    }
    memory.release();
    return unique_request_ptr(builder.getFrame());
}

void createCheckpoint(EngineIface* h) {
    auto request = createPacket(cb::mcbp::ClientOpcode::CreateCheckpoint);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *request, add_response),
            "Failed to create a new checkpoint.");
}

ENGINE_ERROR_CODE del(EngineIface* h,
                      const char* key,
                      uint64_t cas,
                      Vbid vbucket,
                      const void* cookie) {
    mutation_descr_t mut_info{};
    return del(h, key, &cas, vbucket, cookie, &mut_info);
}

ENGINE_ERROR_CODE del(EngineIface* h,
                      const char* key,
                      uint64_t* cas,
                      Vbid vbucket,
                      const void* cookie,
                      mutation_descr_t* mut_info) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }

    auto ret = h->remove(cookie,
                         DocKey(key, DocKeyEncodesCollectionId::No),
                         *cas,
                         vbucket,
                         {},
                         *mut_info);
    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

/** Simplified version of store for handling the common case of performing
 * a delete with a value.
 */
ENGINE_ERROR_CODE delete_with_value(EngineIface* h,
                                    const void* cookie,
                                    uint64_t cas,
                                    const char* key,
                                    cb::const_char_buffer value,
                                    cb::mcbp::Datatype datatype) {
    auto ret = storeCasVb11(h,
                            cookie,
                            OPERATION_SET,
                            key,
                            value.data(),
                            value.size(),
                            9258,
                            cas,
                            Vbid(0),
                            /*exp*/ 0,
                            uint8_t(datatype),
                            DocumentState::Deleted);
    wait_for_flusher_to_settle(h);

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE del_with_meta(EngineIface* h,
                                const char* key,
                                const size_t keylen,
                                const Vbid vb,
                                ItemMetaData* itemMeta,
                                uint64_t cas_for_delete,
                                uint32_t options,
                                const void* cookie,
                                const std::vector<char>& nmeta,
                                protocol_binary_datatype_t datatype,
                                const std::vector<char>& value) {
    RawItemMetaData meta{itemMeta->cas,
                         itemMeta->revSeqno,
                         itemMeta->flags,
                         itemMeta->exptime};
    return del_with_meta(h,
                         key,
                         keylen,
                         vb,
                         &meta,
                         cas_for_delete,
                         options,
                         cookie,
                         nmeta,
                         datatype,
                         value);
}

ENGINE_ERROR_CODE del_with_meta(EngineIface* h,
                                const char* key,
                                const size_t keylen,
                                const Vbid vb,
                                RawItemMetaData* itemMeta,
                                uint64_t cas_for_delete,
                                uint32_t options,
                                const void* cookie,
                                const std::vector<char>& nmeta,
                                protocol_binary_datatype_t datatype,
                                const std::vector<char>& value) {
    size_t blen = 24;
    std::unique_ptr<char[]> ext(new char[30]);
    std::unique_ptr<ExtendedMetaData> emd;

    encodeWithMetaExt(ext.get(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.get() + blen, (char*)&optionsSwapped, sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (nmeta.size() > 0) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.get() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    auto pkt = createPacket(cb::mcbp::ClientOpcode::DelWithMeta,
                            vb,
                            cas_for_delete,
                            {ext.get(), blen},
                            {key, keylen},
                            {value.data(), value.size()},
                            datatype,
                            {nmeta.data(), nmeta.size()});

    return h->unknown_command(cookie, *pkt, add_response_set_del_meta);
}

void evict_key(EngineIface* h,
               const char* key,
               Vbid vbucketId,
               const char* msg,
               bool expectError) {
    int nonResidentItems = get_int_stat(h, "ep_num_non_resident");
    int numEjectedItems = get_int_stat(h, "ep_num_value_ejects");
    auto pkt = createPacket(cb::mcbp::ClientOpcode::EvictKey,
                            vbucketId,
                            0,
                            {},
                            {key, strlen(key)});
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(NULL, *pkt, add_response),
            "Failed to perform CMD_EVICT_KEY.");

    if (expectError) {
        checkeq(cb::mcbp::Status::KeyEexists, last_status.load(),
                "evict_key: expected KEY_EEXISTS when evicting key");
    } else {
        if (last_body != "Already ejected.") {
            nonResidentItems++;
            numEjectedItems++;
        }
        checkeq(cb::mcbp::Status::Success, last_status.load(),
                "evict_key: expected SUCCESS when evicting key.");
    }

    checkeq(nonResidentItems,
            get_int_stat(h, "ep_num_non_resident"),
            "Incorrect number of non-resident items");
    checkeq(numEjectedItems,
            get_int_stat(h, "ep_num_value_ejects"),
            "Incorrect number of ejected items");

    if (msg != NULL && last_body != msg) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_body.c_str());
        abort();
    }
}

ENGINE_ERROR_CODE checkpointPersistence(EngineIface* h,
                                        uint64_t checkpoint_id,
                                        Vbid vb) {
    checkpoint_id = htonll(checkpoint_id);
    auto request =
            createPacket(cb::mcbp::ClientOpcode::CheckpointPersistence,
                         vb,
                         0,
                         {},
                         {},
                         {(const char*)&checkpoint_id, sizeof(uint64_t)});
    ENGINE_ERROR_CODE rv = h->unknown_command(nullptr, *request, add_response);
    return rv;
}

ENGINE_ERROR_CODE seqnoPersistence(EngineIface* h,
                                   const void* cookie,
                                   Vbid vbucket,
                                   uint64_t seqno) {
    seqno = htonll(seqno);
    char buffer[8];
    memcpy(buffer, &seqno, sizeof(uint64_t));
    auto request = createPacket(
            cb::mcbp::ClientOpcode::SeqnoPersistence, vbucket, 0, {buffer, 8});
    return h->unknown_command(cookie, *request, add_response);
}

cb::EngineErrorItemPair gat(EngineIface* h,
                            const char* key,
                            Vbid vb,
                            uint32_t exp) {
    const auto* cookie = testHarness->create_cookie();
    auto ret = h->get_and_touch(
            cookie, DocKey(key, DocKeyEncodesCollectionId::No), vb, exp, {});
    testHarness->destroy_cookie(cookie);

    if (ret.first == cb::engine_errc::success) {
        item_info info;
        check(h->get_item_info(ret.second.get(), &info),
              "gat Failed to get item info");

        last_body.assign((const char*)info.value[0].iov_base,
                         info.value[0].iov_len);
    }
    return ret;
}

bool get_item_info(EngineIface* h, item_info* info, const char* key, Vbid vb) {
    auto ret = get(h, NULL, key, vb);
    if (ret.first != cb::engine_errc::success) {
        return false;
    }
    if (!h->get_item_info(ret.second.get(), info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    return true;
}

cb::EngineErrorItemPair getl(EngineIface* h,
                             const void* cookie,
                             const char* key,
                             Vbid vb,
                             uint32_t lock_timeout) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }
    auto ret = h->get_locked(cookie,
                             DocKey(key, DocKeyEncodesCollectionId::No),
                             vb,
                             lock_timeout);
    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

bool get_meta(EngineIface* h,
              const char* key,
              const void* cookie) {
    cb::EngineErrorMetadataPair out;

    return get_meta(h, key, out, cookie);
}

bool get_meta(EngineIface* h,
              const char* key,
              cb::EngineErrorMetadataPair& out,
              const void* cookie) {
    DocKey docKey(key, DocKeyEncodesCollectionId::No);
    bool cookie_create = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        cookie_create = true;
    }

    out = h->get_meta(cookie, docKey, Vbid(0));

    if (cookie_create) {
        testHarness->destroy_cookie(cookie);
    }

    return out.first == cb::engine_errc::success;
}

ENGINE_ERROR_CODE observe(EngineIface* h, std::map<std::string, Vbid> obskeys) {
    std::stringstream value;
    std::map<std::string, Vbid>::iterator it;
    for (it = obskeys.begin(); it != obskeys.end(); ++it) {
        Vbid vb = it->second.hton();
        uint16_t keylen = htons(it->first.length());
        value.write((char*)&vb, sizeof(Vbid));
        value.write((char*) &keylen, sizeof(uint16_t));
        value.write(it->first.c_str(), it->first.length());
    }

    auto request = createPacket(
            cb::mcbp::ClientOpcode::Observe, Vbid(0), 0, {}, {}, value.str());

    return h->unknown_command(nullptr, *request, add_response);
}

ENGINE_ERROR_CODE observe_seqno(EngineIface* h, Vbid vb_id, uint64_t uuid) {
    uint64_t vb_uuid = htonll(uuid);
    std::stringstream data;
    data.write((char *) &vb_uuid, sizeof(uint64_t));

    auto request = createPacket(
            cb::mcbp::ClientOpcode::ObserveSeqno, vb_id, 0, {}, {}, data.str());
    return h->unknown_command(nullptr, *request, add_response);
}

void get_replica(EngineIface* h, const char* key, Vbid vbid) {
    auto request = createPacket(cb::mcbp::ClientOpcode::GetReplica,
                                vbid,
                                0,
                                {},
                                {key, strlen(key)});
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *request, add_response),
            "Get Replica Failed");
}

unique_request_ptr prepare_get_replica(EngineIface* h,
                                       vbucket_state_t state,
                                       bool makeinvalidkey) {
    Vbid id(0);
    const char *key = "k0";
    auto request = createPacket(
            cb::mcbp::ClientOpcode::GetReplica, id, 0, {}, {key, strlen(key)});

    if (!makeinvalidkey) {
        checkeq(ENGINE_SUCCESS,
                store(h,
                      nullptr,
                      OPERATION_SET,
                      key,
                      "replicadata",
                      nullptr,
                      0,
                      id),
                "Get Replica Failed");

        check(set_vbucket_state(h, id, state),
              "Failed to set vbucket active state, Get Replica Failed");
    }

    return request;
}

bool set_param(EngineIface* h,
               cb::mcbp::request::SetParamPayload::Type paramtype,
               const char* param,
               const char* val,
               Vbid vb) {
    cb::mcbp::request::SetParamPayload payload;
    payload.setParamType(paramtype);
    auto buffer = payload.getBuffer();
    auto request = createPacket(
            cb::mcbp::ClientOpcode::SetParam,
            vb,
            0,
            {reinterpret_cast<const char*>(buffer.data()), buffer.size()},
            {param, strlen(param)},
            {val, strlen(val)});

    if (h->unknown_command(nullptr, *request, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == cb::mcbp::Status::Success;
}

bool set_vbucket_state(EngineIface* h, Vbid vb, vbucket_state_t state) {
    char ext[4];
    encodeExt(ext, static_cast<uint32_t>(state));
    auto request =
            createPacket(cb::mcbp::ClientOpcode::SetVbucket, vb, 0, {ext, 4});

    if (h->unknown_command(nullptr, *request, add_response) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == cb::mcbp::Status::Success;
}

bool get_all_vb_seqnos(EngineIface* h,
                       boost::optional<vbucket_state_t> state,
                       const void* cookie,
                       boost::optional<CollectionIDType> collection) {
    unique_request_ptr pkt;

    if (collection) {
        if (!state) {
            // Do the same check so we can print for the user...
            checkeq(state.is_initialized(),
                    true,
                    "State must be set when "
                    "collection is specified");
            return false;
        }

        char ext[sizeof(vbucket_state_t) + sizeof(CollectionIDType)];
        encodeExt(ext, static_cast<uint32_t>(*state));
        encodeExt(ext, *collection, sizeof(vbucket_state_t));
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                           Vbid(0),
                           0,
                           {ext,sizeof(vbucket_state_t) + sizeof(CollectionIDType)});
    } else if (state) {
        char ext[sizeof(vbucket_state_t)];
        encodeExt(ext, static_cast<uint32_t>(*state));
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                           Vbid(0),
                           0,
                           {ext, sizeof(vbucket_state_t)});
    } else {
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos);
    }

    checkeq(ENGINE_SUCCESS,
            h->unknown_command(cookie, *pkt, add_response),
            "Error in getting all vb info");

    return last_status == cb::mcbp::Status::Success;
}

void verify_all_vb_seqnos(EngineIface* h,
                          int vb_start,
                          int vb_end,
                          boost::optional<CollectionID> cid) {
    const int per_vb_resp_size = sizeof(uint16_t) + sizeof(uint64_t);
    const int high_seqno_offset = sizeof(uint16_t);

    /* Check if the total response length is as expected. We expect 10 bytes
     (2 for vb_id + 8 for seqno) */
    checkeq((vb_end - vb_start + 1) * per_vb_resp_size,
            static_cast<int>(last_body.size()),
            "Failed to get all vb info.");
    /* Check if the contents are correct */
    for (int i = 0; i < (vb_end - vb_start + 1); i++) {
        /* Check for correct vb_id */
        checkeq(static_cast<const uint16_t>(vb_start + i),
                ntohs(*(reinterpret_cast<const uint16_t*>(last_body.data() +
                                                          per_vb_resp_size*i))),
              "vb_id mismatch");

        uint64_t high_seqno_vb;
        if (cid) {
            // Get high seqno for the collection in the vBucket
            std::string vb_stat_seqno("vb_" + std::to_string(vb_start + i) +
                                      ":collection:" + cid->to_string() +
                                      ":entry:high_seqno");
            high_seqno_vb = get_ull_stat(
                    h, vb_stat_seqno.c_str(), "collections-details");
        } else {
            // Get high seqno for the vBucket
            std::string vb_stat_seqno("vb_" + std::to_string(vb_start + i) +
                                      ":high_seqno");
            high_seqno_vb =
                    get_ull_stat(h, vb_stat_seqno.c_str(), "vbucket-seqno");
        }

        checkeq(high_seqno_vb,
                ntohll(*(reinterpret_cast<const uint64_t*>(last_body.data() +
                                                           per_vb_resp_size*i +
                                                           high_seqno_offset))),
                "high_seqno mismatch");
    }
}

static ENGINE_ERROR_CODE store_with_meta(EngineIface* h,
                                         cb::mcbp::ClientOpcode cmd,
                                         const char* key,
                                         const size_t keylen,
                                         const char* val,
                                         const size_t vallen,
                                         const Vbid vb,
                                         ItemMetaData* itemMeta,
                                         uint64_t cas_for_store,
                                         uint32_t options,
                                         uint8_t datatype,
                                         const void* cookie,
                                         const std::vector<char>& nmeta) {
    size_t blen = 24;
    std::unique_ptr<char[]> ext(new char[30]);
    std::unique_ptr<ExtendedMetaData> emd;

    encodeWithMetaExt(ext.get(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.get() + blen, (char*)&optionsSwapped, sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (nmeta.size() > 0) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.get() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    auto request = createPacket(cmd,
                                vb,
                                cas_for_store,
                                {ext.get(), blen},
                                {key, keylen},
                                {val, vallen},
                                datatype,
                                {nmeta.data(), nmeta.size()});

    return h->unknown_command(cookie, *request, add_response_set_del_meta);
}

ENGINE_ERROR_CODE set_with_meta(EngineIface* h,
                                const char* key,
                                const size_t keylen,
                                const char* val,
                                const size_t vallen,
                                const Vbid vb,
                                ItemMetaData* itemMeta,
                                uint64_t cas_for_set,
                                uint32_t options,
                                uint8_t datatype,
                                const void* cookie,
                                const std::vector<char>& nmeta) {
    return store_with_meta(h,
                           cb::mcbp::ClientOpcode::SetWithMeta,
                           key,
                           keylen,
                           val,
                           vallen,
                           vb,
                           itemMeta,
                           cas_for_set,
                           options,
                           datatype,
                           cookie,
                           nmeta);
}

ENGINE_ERROR_CODE add_with_meta(EngineIface* h,
                                const char* key,
                                const size_t keylen,
                                const char* val,
                                const size_t vallen,
                                const Vbid vb,
                                ItemMetaData* itemMeta,
                                uint64_t cas_for_add,
                                uint32_t options,
                                uint8_t datatype,
                                const void* cookie,
                                const std::vector<char>& nmeta) {
    return store_with_meta(h,
                           cb::mcbp::ClientOpcode::AddWithMeta,
                           key,
                           keylen,
                           val,
                           vallen,
                           vb,
                           itemMeta,
                           cas_for_add,
                           options,
                           datatype,
                           cookie,
                           nmeta);
}

static ENGINE_ERROR_CODE return_meta(EngineIface* h,
                                     const char* key,
                                     const size_t keylen,
                                     const char* val,
                                     const size_t vallen,
                                     const Vbid vb,
                                     const uint64_t cas,
                                     const uint32_t flags,
                                     const uint32_t exp,
                                     cb::mcbp::request::ReturnMetaType type,
                                     uint8_t datatype,
                                     const void* cookie) {
    cb::mcbp::request::ReturnMetaPayload meta;
    meta.setMutationType(type);
    meta.setFlags(flags);
    meta.setExpiration(exp);

    auto pkt =
            createPacket(cb::mcbp::ClientOpcode::ReturnMeta,
                         vb,
                         cas,
                         {reinterpret_cast<const char*>(&meta), sizeof(meta)},
                         {key, keylen},
                         {val, vallen},
                         datatype);
    return h->unknown_command(cookie, *pkt, add_response_ret_meta);
}

ENGINE_ERROR_CODE set_ret_meta(EngineIface* h,
                               const char* key,
                               const size_t keylen,
                               const char* val,
                               const size_t vallen,
                               const Vbid vb,
                               const uint64_t cas,
                               const uint32_t flags,
                               const uint32_t exp,
                               uint8_t datatype,
                               const void* cookie) {
    return return_meta(h,
                       key,
                       keylen,
                       val,
                       vallen,
                       vb,
                       cas,
                       flags,
                       exp,
                       cb::mcbp::request::ReturnMetaType::Set,
                       datatype,
                       cookie);
}

ENGINE_ERROR_CODE add_ret_meta(EngineIface* h,
                               const char* key,
                               const size_t keylen,
                               const char* val,
                               const size_t vallen,
                               const Vbid vb,
                               const uint64_t cas,
                               const uint32_t flags,
                               const uint32_t exp,
                               uint8_t datatype,
                               const void* cookie) {
    return return_meta(h,
                       key,
                       keylen,
                       val,
                       vallen,
                       vb,
                       cas,
                       flags,
                       exp,
                       cb::mcbp::request::ReturnMetaType::Add,
                       datatype,
                       cookie);
}

ENGINE_ERROR_CODE del_ret_meta(EngineIface* h,
                               const char* key,
                               const size_t keylen,
                               const Vbid vb,
                               const uint64_t cas,
                               const void* cookie) {
    return return_meta(h,
                       key,
                       keylen,
                       NULL,
                       0,
                       vb,
                       cas,
                       0,
                       0,
                       cb::mcbp::request::ReturnMetaType::Del,
                       0x00,
                       cookie);
}

void disable_traffic(EngineIface* h) {
    auto pkt = createPacket(cb::mcbp::ClientOpcode::DisableTraffic);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Failed to disable data traffic");
}

void enable_traffic(EngineIface* h) {
    auto pkt = createPacket(cb::mcbp::ClientOpcode::EnableTraffic);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *pkt, add_response),
            "Failed to send data traffic command to the server");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Failed to enable data traffic");
}

void start_persistence(EngineIface* h) {
    if (!isPersistentBucket(h)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    auto pkt = createPacket(cb::mcbp::ClientOpcode::StartPersistence);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *pkt, add_response),
            "Failed to stop persistence.");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Error starting persistence.");
}

void stop_persistence(EngineIface* h) {
    if (!isPersistentBucket(h)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    useconds_t sleepTime = 128;
    while (true) {
        if (get_str_stat(h, "ep_flusher_state", 0) == "running") {
            break;
        }
        decayingSleep(&sleepTime);
    }

    auto pkt = createPacket(cb::mcbp::ClientOpcode::StopPersistence);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(nullptr, *pkt, add_response),
            "Failed to stop persistence.");
    checkeq(cb::mcbp::Status::Success,
            last_status.load(),
            "Error stopping persistence.");
}

ENGINE_ERROR_CODE store(EngineIface* h,
                        const void* cookie,
                        ENGINE_STORE_OPERATION op,
                        const char* key,
                        const char* value,
                        item** outitem,
                        uint64_t casIn,
                        Vbid vb,
                        uint32_t exp,
                        uint8_t datatype,
                        DocumentState docState) {
    auto ret = storeCasVb11(h,
                            cookie,
                            op,
                            key,
                            value,
                            strlen(value),
                            9258,
                            casIn,
                            vb,
                            exp,
                            datatype,
                            docState);
    if (outitem) {
        *outitem = ret.second.release();
    }
    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE storeCasOut(EngineIface* h,
                              const void* cookie,
                              Vbid vb,
                              const std::string& key,
                              const std::string& value,
                              protocol_binary_datatype_t datatype,
                              item*& out_item,
                              uint64_t& out_cas,
                              DocumentState docState) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }

    auto ret = allocate(h, cookie, key, value.size(), 0, 0, datatype, vb);
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");
    item_info info;
    check(h->get_item_info(ret.second.get(), &info), "Unable to get item_info");
    memcpy(info.value[0].iov_base, value.data(), value.size());
    ENGINE_ERROR_CODE res = h->store(
            cookie, ret.second.get(), out_cas, OPERATION_SET, {}, docState);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return res;
}

cb::EngineErrorItemPair storeCasVb11(EngineIface* h,
                                     const void* cookie,
                                     ENGINE_STORE_OPERATION op,
                                     const char* key,
                                     const char* value,
                                     size_t vlen,
                                     uint32_t flags,
                                     uint64_t casIn,
                                     Vbid vb,
                                     uint32_t exp,
                                     uint8_t datatype,
                                     DocumentState docState) {
    uint64_t cas = 0;

    auto rv = allocate(h, cookie, key, vlen, flags, exp, datatype, vb);
    if (rv.first != cb::engine_errc::success) {
        return rv;
    }
    item_info info;
    if (!h->get_item_info(rv.second.get(), &info)) {
        abort();
    }

    cb_assert(info.value[0].iov_len == vlen);
    std::copy(value, value + vlen, reinterpret_cast<char*>(info.value[0].iov_base));
    h->item_set_cas(rv.second.get(), casIn);

    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }

    auto storeRet = h->store(cookie, rv.second.get(), cas, op, {}, docState);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return {cb::engine_errc(storeRet), std::move(rv.second)};
}

ENGINE_ERROR_CODE touch(EngineIface* h,
                        const char* key,
                        Vbid vb,
                        uint32_t exp) {
    const auto* cookie = testHarness->create_cookie();
    auto result = h->get_and_touch(
            cookie, DocKey(key, DocKeyEncodesCollectionId::No), vb, exp, {});
    testHarness->destroy_cookie(cookie);

    // Update the global cas value (used by some tests)
    if (result.first == cb::engine_errc::success) {
        item_info info{};
        check(h->get_item_info(result.second.get(), &info),
              "Failed to get item info");
        last_cas.store(info.cas);
    }

    return ENGINE_ERROR_CODE(result.first);
}

ENGINE_ERROR_CODE unl(EngineIface* h,
                      const void* cookie,
                      const char* key,
                      Vbid vb,
                      uint64_t cas) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }
    auto ret = h->unlock(
            cookie, DocKey(key, DocKeyEncodesCollectionId::No), vb, cas);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }
    return ret;
}

void compact_db(EngineIface* h,
                const Vbid vbucket_id,
                const Vbid db_file_id,
                const uint64_t purge_before_ts,
                const uint64_t purge_before_seq,
                const uint8_t drop_deletes) {
    cb::mcbp::request::CompactDbPayload payload;
    payload.setPurgeBeforeTs(purge_before_ts);
    payload.setPurgeBeforeSeq(purge_before_seq);
    payload.setDropDeletes(drop_deletes);
    payload.setDbFileId(db_file_id);

    auto pkt = createPacket(
            cb::mcbp::ClientOpcode::CompactDb,
            vbucket_id,
            0,
            {reinterpret_cast<const char*>(&payload), sizeof(payload)});
    auto ret = h->unknown_command(nullptr, *pkt, add_response);

    const auto backend = get_str_stat(h, "ep_backend");

    if (backend == "couchdb" || backend == "magma") {
        if (ret == ENGINE_ENOTSUP) {
            // Ephemeral buckets return ENGINE_ENOTSUP, and this method is
            // called from a lot of the test cases we run on all bucket
            // types. Lets remap the error code to success
            ret = ENGINE_SUCCESS;
        }
        checkeq(ENGINE_SUCCESS, ret, "Failed to request compact vbucket");
    } else {
        checkeq(ENGINE_FAILED,
                ret,
                "checkForDBExistence returns ENGINE_FAILED for !couchdb");
    }
}

ENGINE_ERROR_CODE vbucketDelete(EngineIface* h, Vbid vb, const char* args) {
    uint32_t argslen = args ? strlen(args) : 0;
    auto pkt = createPacket(
            cb::mcbp::ClientOpcode::DelVbucket, vb, 0, {}, {}, {args, argslen});

    return h->unknown_command(nullptr, *pkt, add_response);
}

ENGINE_ERROR_CODE verify_key(EngineIface* h, const char* key, Vbid vbucket) {
    auto rv = get(h, NULL, key, vbucket);
    return ENGINE_ERROR_CODE(rv.first);
}

std::pair<ENGINE_ERROR_CODE, std::string> get_value(EngineIface* h,
                                                    const void* cookie,
                                                    const char* key,
                                                    Vbid vbucket,
                                                    DocStateFilter state) {
    auto rv = get(h, cookie, key, vbucket, state);
    if (rv.first != cb::engine_errc::success) {
        return {ENGINE_ERROR_CODE(rv.first), ""};
    }
    item_info info;
    if (!h->get_item_info(rv.second.get(), &info)) {
        return {ENGINE_FAILED, ""};
    }
    auto value = std::string(reinterpret_cast<char*>(info.value[0].iov_base),
                             info.value[0].iov_len);
    return make_pair(ENGINE_ERROR_CODE(rv.first), value);
}

bool verify_vbucket_missing(EngineIface* h, Vbid vb) {
    const auto vbstr = "vb_" + std::to_string(vb.get());

    // Try up to three times to verify the bucket is missing.  Bucket
    // state changes are async.
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }

    const auto* cookie = testHarness->create_cookie();
    checkeq(ENGINE_SUCCESS,
            h->get_stats(cookie, {}, add_stats),
            "Failed to get stats.");
    testHarness->destroy_cookie(cookie);

    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        if (vals.find(vbstr) == vals.end()) {
            return true;
        }

        std::cerr << "Expected bucket missing, got " <<
                vals[vbstr] << std::endl;
    }
    return false;
}

bool verify_vbucket_state(EngineIface* h,
                          Vbid vb,
                          vbucket_state_t expected,
                          bool mute) {
    auto pkt = createPacket(cb::mcbp::ClientOpcode::GetVbucket, vb, 0);

    ENGINE_ERROR_CODE errcode = h->unknown_command(NULL, *pkt, add_response);
    if (errcode != ENGINE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Error code when getting vbucket %d\n", errcode);
        }
        return false;
    }

    if (last_status != cb::mcbp::Status::Success) {
        if (!mute) {
            fprintf(stderr, "Last protocol status was %s (%s)\n",
                    to_string(last_status.load()).c_str(),
                    last_body.size() > 0 ? last_body.c_str() : "unknown");
        }
        return false;
    }

    vbucket_state_t state;
    memcpy(&state, last_body.data(), sizeof(state));
    state = static_cast<vbucket_state_t>(ntohl(state));
    return state == expected;
}

void sendDcpAck(EngineIface* h,
                const void* cookie,
                cb::mcbp::ClientOpcode opcode,
                cb::mcbp::Status status,
                uint32_t opaque) {
    protocol_binary_response_header pkt;
    pkt.response.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt.response.setOpcode(opcode);
    pkt.response.setStatus(status);
    pkt.response.setOpaque(opaque);

    auto& dcp = dynamic_cast<DcpIface&>(*h);
    checkeq(ENGINE_SUCCESS, dcp.response_handler(cookie, &pkt),
          "Expected success");
}

class engine_error : public std::exception {
public:
    engine_error(ENGINE_ERROR_CODE code_)
        : code(code_) {}

    virtual const char* what() const NOEXCEPT {
        return "engine_error";
    }

    ENGINE_ERROR_CODE code;
};

/* The following set of functions get a given stat as the specified type
 * (int, float, unsigned long, string, bool, etc).
 * If the engine->get_stats() call fails, throws a engine_error exception.
 * If the given statname doesn't exist under the given statname, throws a
 * std::out_of_range exception.
 */
template <>
int get_stat(EngineIface* h,
             const char* statname,
             const char* statkey) {
    return std::stoi(get_str_stat(h, statname, statkey));
}
template <>
uint64_t get_stat(EngineIface* h,
                  const char* statname,
                  const char* statkey) {
    return std::stoull(get_str_stat(h, statname, statkey));
}

template <>
bool get_stat(EngineIface* h,
              const char* statname,
              const char* statkey) {
    return get_str_stat(h, statname, statkey) == "true";
}

template <>
std::string get_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    std::lock_guard<std::mutex> lh(get_stat_context.mutex);

    get_stat_context.requested_stat_name = statname;
    get_stat_context.actual_stat_value.clear();

    const auto* cookie = testHarness->create_cookie();
    ENGINE_ERROR_CODE err =
            h->get_stats(cookie,
                         {statkey, statkey == NULL ? 0 : strlen(statkey)},
                         add_individual_stat);
    testHarness->destroy_cookie(cookie);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }

    if (get_stat_context.actual_stat_value.empty()) {
        throw std::out_of_range(std::string("Failed to find requested statname '") +
                                statname + "'");
    }

    // Here we are explictly forcing a copy of the object to work
    // around std::string copy-on-write data-race issues seen on some
    // versions of libstdc++ - see MB-18510 / MB-19688.
    return std::string(get_stat_context.actual_stat_value.begin(),
                       get_stat_context.actual_stat_value.end());
}

/// Backward-compatible functions (encode type name in function name).
int get_int_stat(EngineIface* h,
                 const char* statname,
                 const char* statkey) {
    return get_stat<int>(h, statname, statkey);
}

float get_float_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    return std::stof(get_str_stat(h, statname, statkey));
}

uint32_t get_ul_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    return std::stoul(get_str_stat(h, statname, statkey));
}

uint64_t get_ull_stat(EngineIface* h,
                      const char* statname,
                      const char* statkey) {
    return get_stat<uint64_t>(h, statname, statkey);
}

std::string get_str_stat(EngineIface* h,
                         const char* statname,
                         const char* statkey) {
    return get_stat<std::string>(h, statname, statkey);
}

bool get_bool_stat(EngineIface* h,
                   const char* statname,
                   const char* statkey) {
    const auto s = get_str_stat(h, statname, statkey);

    if (s == "true") {
        return true;
    } else if (s == "false") {
        return false;
    } else {
        throw std::invalid_argument("Unable to convert string '" + s + "' to type bool");
    }
}

/* Fetches the value for a given statname in the given statkey set.
 * @return te value of statname, or default_value if that statname was not
 * found.
 */
int get_int_stat_or_default(EngineIface* h,
                            int default_value,
                            const char* statname,
                            const char* statkey) {
    try {
        return get_int_stat(h, statname, statkey);
    } catch (std::out_of_range&) {
        return default_value;
    }
}

uint64_t get_histo_stat(EngineIface* h,
                        const char* statname,
                        const char* statkey,
                        const Histo_stat_info histo_info) {
    std::lock_guard<std::mutex> lh(get_stat_context.mutex);

    get_stat_context.histogram_stat_int_value = new HistogramStats<uint64_t>();
    get_histo_stat(h, statname, statkey);

    /* Get the necessary info from the histogram */
    uint64_t ret_val = 0;
    switch (histo_info) {
        case Histo_stat_info::TOTAL_COUNT:
            ret_val = get_stat_context.histogram_stat_int_value->total();
            break;
        case Histo_stat_info::NUM_BINS:
            ret_val =
                    static_cast<uint64_t>(get_stat_context.
                                          histogram_stat_int_value->num_bins());
            break;
    }

    delete get_stat_context.histogram_stat_int_value;
    return ret_val;
}

static void get_histo_stat(EngineIface* h,
                           const char* statname,
                           const char* statkey) {
    get_stat_context.requested_stat_name = statname;
    /* Histo stats for tasks are append as task_name_START,END.
       Hence append _ */
    get_stat_context.requested_stat_name.append("_");

    const auto* cookie = testHarness->create_cookie();
    auto err = h->get_stats(cookie,
                            {statkey, statkey == NULL ? 0 : strlen(statkey)},
                            add_individual_histo_stat);
    testHarness->destroy_cookie(cookie);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }
}

statistic_map get_all_stats(EngineIface* h, const char* statset) {
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }
    const auto* cookie = testHarness->create_cookie();
    auto err = h->get_stats(cookie,
                            {statset, statset == NULL ? 0 : strlen(statset)},
                            add_stats);
    testHarness->destroy_cookie(cookie);

    if (err != ENGINE_SUCCESS) {
        throw engine_error(err);
    }

    std::lock_guard<std::mutex> lh(vals_mutex);
    return vals;
}

void verify_curr_items(EngineIface* h,
                       int exp,
                       const char* msg) {
    int curr_items = get_int_stat(h, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

void wait_for_stat_to_be_gte(EngineIface* h,
                             const char* stat,
                             int final,
                             const char* stat_key,
                             const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be greater or equal than", stat,
                                         stat_key, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, stat, stat_key);
        if (current >= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_stat_to_be_lte(EngineIface* h,
                             const char* stat,
                             int final,
                             const char* stat_key,
                             const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be less than or equal to", stat,
                                         stat_key, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, stat, stat_key);
        if (current <= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_expired_items_to_be(EngineIface* h,
                                  int final,
                                  const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be", "expired items",
                                         NULL, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, "ep_expired_access") +
                       get_int_stat(h, "ep_expired_compactor") +
                       get_int_stat(h, "ep_expired_pager");
        if (current == final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_memory_usage_below(EngineIface* h,
                                 int mem_threshold,
                                 const time_t max_wait_time_in_secs) {
    useconds_t sleepTime = 128;
    WaitTimeAccumulator<int> accumulator("to be below", "mem_used", NULL,
                                         mem_threshold,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, "mem_used");
        if (current <= mem_threshold) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

bool wait_for_warmup_complete(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return true;
    }

    useconds_t sleepTime = 128;
    do {
        try {
            if (get_str_stat(h, "ep_warmup_thread", "warmup") == "complete") {
                return true;
            }
        } catch (engine_error&) {
            // If the stat call failed then the warmup stats group no longer
            // exists and hence warmup is complete.
            return true;
        }
        decayingSleep(&sleepTime);
    } while(true);
}

void wait_for_flusher_to_settle(EngineIface* h) {
    wait_for_stat_to_be(h, "ep_queue_size", 0);

    /* check that vb backfill queue is empty as well */
    checkeq(0,
            get_int_stat(h, "ep_vb_backfill_queue_size", 0),
            "even though disk queue is empty, vb backfill queue is not!!");

    if (!isPersistentBucket(h)) {
        // We don't run flusher in non-persistent buckets
        return;
    }
    // We also need to to wait for any outstanding flushes to disk to
    // complete - specifically so when in full eviction mode we have
    // waited for the item counts in each vBucket to be synced with
    // the number of items on disk. See
    // EPBucket::commit().
    wait_for_stat_to_be(h, "ep_flusher_todo", 0);
}

void wait_for_item_compressor_to_settle(EngineIface* h) {
    int visited_count = get_int_stat(h, "ep_item_compressor_num_visited");

    // We need to wait for at least one more run of the item compressor
    wait_for_stat_to_be(h, "ep_item_compressor_num_visited", visited_count + 1);
}

void wait_for_rollback_to_finish(EngineIface* h) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, "ep_rollback_count") == 0) {
        decayingSleep(&sleepTime);
    }
}

void wait_for_persisted_value(EngineIface* h,
                              const char* key,
                              const char* val,
                              Vbid vbucketId) {
    int commitNum = 0;
    if (isPersistentBucket(h)) {
        commitNum = get_int_stat(h, "ep_commit_num");
    }
    checkeq(ENGINE_SUCCESS,
            store(h, nullptr, OPERATION_SET, key, val, nullptr, 0, vbucketId),
            "Failed to store an item.");

    if (isPersistentBucket(h)) {
        // Wait for persistence...
        wait_for_flusher_to_settle(h);
        wait_for_stat_change(h, "ep_commit_num", commitNum);
    }
}

void abort_msg(const char* expr, const char* msg, const char* file, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            file, line, msg, expr);
    throw TestExpectationFailed();
}

/* Helper function to validate the return from store() */
void validate_store_resp(ENGINE_ERROR_CODE ret, int& num_items)
{
    switch (ret) {
        case ENGINE_SUCCESS:
            num_items++;
            break;
        case ENGINE_TMPFAIL:
            /* TMPFAIL means we are hitting high memory usage; retry */
            break;
#ifdef EP_USE_MAGMA
        case ENGINE_ENOMEM:
            break;
#endif
        default:
            check(false,
                  ("write_items_upto_mem_perc: Unexpected response from "
                   "store(): " + std::to_string(ret)).c_str());
            break;
    }
}

void write_items(EngineIface* h,
                 int num_items,
                 int start_seqno,
                 const char* key_prefix,
                 const char* value,
                 uint32_t expiry,
                 Vbid vb,
                 DocumentState docState) {
    int j = 0;
    while (1) {
        if (j == num_items) {
            break;
        }
        std::string key(key_prefix + std::to_string(j + start_seqno));
        ENGINE_ERROR_CODE ret = store(h,
                                      nullptr,
                                      OPERATION_SET,
                                      key.c_str(),
                                      value,
                                      nullptr,
                                      /*cas*/ 0,
                                      vb,
                                      expiry,
                                      0,
                                      docState);
        validate_store_resp(ret, j);
    }
}

/* Helper function to write unique items starting from keyXX until memory usage
   hits "mem_thresh_perc" (XX is start_seqno) */
int write_items_upto_mem_perc(EngineIface* h,
                              int mem_thresh_perc,
                              int start_seqno,
                              const char* key_prefix,
                              const char* value) {
    float maxSize =
            static_cast<float>(get_int_stat(h, "ep_max_size", "memory"));
    float mem_thresh = static_cast<float>(mem_thresh_perc) / (100.0);
    int num_items = 0;
    while (1) {
        /* Load items into server until mem_thresh_perc of the mem quota
         is used. Getting stats is expensive, only check every 100
         iterations. */
        if ((num_items % 100) == 0) {
            float memUsed = float(get_int_stat(h, "mem_used", "memory"));
            if (memUsed > (maxSize * mem_thresh)) {
                /* Persist all items written so far. */
                break;
            }
        }
        std::string key("key" + std::to_string(num_items + start_seqno));
        ENGINE_ERROR_CODE ret =
                store(h, nullptr, OPERATION_SET, key.c_str(), "somevalue");
        validate_store_resp(ret, num_items);
    }
    return num_items;
}

uint64_t get_CAS(EngineIface* h, const std::string& key) {
    auto ret = get(h, nullptr, key, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "get_CAS: Failed to get key");

    item_info info;
    check(h->get_item_info(ret.second.get(), &info),
          "get_CAS: Failed to get item info for key");

    return info.cas;
}

cb::EngineErrorItemPair allocate(EngineIface* h,
                                 const void* cookie,
                                 const std::string& key,
                                 size_t nbytes,
                                 int flags,
                                 rel_time_t exptime,
                                 uint8_t datatype,
                                 Vbid vb) {
    bool cookie_created = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        cookie_created = true;
    }
    auto ret = h->allocate(cookie,
                           DocKey(key, DocKeyEncodesCollectionId::No),
                           nbytes,
                           flags,
                           exptime,
                           datatype,
                           vb);
    if (cookie_created) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

cb::EngineErrorItemPair get(EngineIface* h,
                            const void* cookie,
                            const std::string& key,
                            Vbid vb,
                            DocStateFilter documentStateFilter) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie();
        create_cookie = true;
    }

    auto ret = h->get(cookie,
                      DocKey(key, DocKeyEncodesCollectionId::No),
                      vb,
                      documentStateFilter);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }
    return ret;
}

bool repeat_till_true(std::function<bool()> functor,
                      uint16_t max_repeat,
                      std::chrono::microseconds sleepTime) {
    bool fSuccess = false;
    do {
        fSuccess = functor();
        if (!fSuccess) {
            std::this_thread::sleep_for(sleepTime);
            max_repeat--;
        }
    } while (!fSuccess && max_repeat > 0);
    return fSuccess;
}

void reset_stats(gsl::not_null<EngineIface*> h) {
    const auto* cookie = testHarness->create_cookie();
    h->reset_stats(cookie);
    testHarness->destroy_cookie(cookie);
}

ENGINE_ERROR_CODE get_stats(gsl::not_null<EngineIface*> h,
                            cb::const_char_buffer key,
                            const AddStatFn& callback) {
    const auto* cookie = testHarness->create_cookie();
    auto ret = h->get_stats(cookie, key, callback);
    testHarness->destroy_cookie(cookie);
    return ret;
}

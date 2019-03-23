/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "ep_testsuite_common.h"
#include "ep_test_apis.h"

#include <cstring>
#include <iostream>
#include <string>
#include <sstream>

#include <sys/stat.h>

#include <platform/cb_malloc.h>
#include <platform/compress.h>
#include <platform/dirutils.h>

const char *dbname_env = NULL;

static enum test_result skipped_test_function(EngineIface* h);

BaseTestCase::BaseTestCase(const char *_name, const char *_cfg, bool _skip)
  : name(_name),
    cfg(_cfg),
    skip(_skip) {
}

BaseTestCase::BaseTestCase(const BaseTestCase &o)
  : name(o.name),
    cfg(o.cfg),
    skip(o.skip) {

    memset(&test, 0, sizeof(test));
    test = o.test;
}

TestCase::TestCase(const char* _name,
                   enum test_result (*_tfun)(EngineIface*),
                   bool (*_test_setup)(EngineIface*),
                   bool (*_test_teardown)(EngineIface*),
                   const char* _cfg,
                   enum test_result (*_prepare)(engine_test_t* test),
                   void (*_cleanup)(engine_test_t* test,
                                    enum test_result result),
                   bool _skip)
    : BaseTestCase(_name, _cfg, _skip) {
    memset(&test, 0, sizeof(test));
    test.tfun = _tfun;
    test.test_setup = _test_setup;
    test.test_teardown = _test_teardown;
    test.prepare = _prepare;
    test.cleanup = _cleanup;
}

TestCaseV2::TestCaseV2(const char *_name,
                       enum test_result(*_tfun)(engine_test_t *),
                       bool(*_test_setup)(engine_test_t *),
                       bool(*_test_teardown)(engine_test_t *),
                       const char *_cfg,
                       enum test_result (*_prepare)(engine_test_t *test),
                       void (*_cleanup)(engine_test_t *test, enum test_result result),
                       bool _skip)
  : BaseTestCase(_name, _cfg, _skip) {

    memset(&test, 0, sizeof(test));
    test.api_v2.tfun = _tfun;
    test.api_v2.test_setup = _test_setup;
    test.api_v2.test_teardown = _test_teardown;
    test.prepare = _prepare;
    test.cleanup = _cleanup;
}

engine_test_t* BaseTestCase::getTest() {
    engine_test_t *ret = &test;

    std::string nm(name);
    std::stringstream ss;

    if (cfg != 0) {
        ss << cfg << ";";
    } else {
        ss << ";";
    }

    // Default to the suite's dbname if the test config didn't already
    // specify it.
    if ((cfg == nullptr) ||
        (std::string(cfg).find("dbname=") == std::string::npos)) {
        ss << "dbname=" << default_dbname << ";";
    }

    if (skip) {
        nm.append(" (skipped)");
        ret->tfun = skipped_test_function;
    } else {
        nm.append(" (couchstore)");
    }

    ret->name = cb_strdup(nm.c_str());
    std::string config = ss.str();
    if (config.length() == 0) {
        ret->cfg = 0;
    } else {
        ret->cfg = cb_strdup(config.c_str());
    }

    return ret;
}

static enum test_result skipped_test_function(EngineIface*) {
    return SKIPPED;
}

enum test_result rmdb(const char* path) {
    try {
        cb::io::rmrf(path);
    } catch (std::system_error& e) {
        throw e;
    }
    if (cb::io::isDirectory(path) || cb::io::isFile(path)) {
        std::cerr << "Failed to remove: " << path << " " << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

bool test_setup(EngineIface* h) {
    wait_for_warmup_complete(h);

    check(set_vbucket_state(h, Vbid(0), vbucket_state_active),
          "Failed to set VB0 state.");

    const auto bucket_type = get_str_stat(h, "ep_bucket_type");
    if (bucket_type == "persistent") {
        // Wait for vb0's state (active) to be persisted to disk, that way
        // we know the KVStore files exist on disk.
        wait_for_stat_to_be_gte(h, "ep_persist_vbstate_total", 1);
    } else if (bucket_type == "ephemeral") {
        // No persistence to wait for here.
    } else {
        check(false,
              (std::string("test_setup: unknown bucket_type '") + bucket_type +
               "' - cannot continue.")
                      .c_str());
        return false;
    }

    // warmup is complete, notify ep engine that it must now enable
    // data traffic
    auto request = createPacket(cb::mcbp::ClientOpcode::EnableTraffic);
    checkeq(ENGINE_SUCCESS,
            h->unknown_command(NULL, *request, add_response),
            "Failed to enable data traffic");

    return true;
}

bool teardown(EngineIface*) {
    vals.clear();
    return true;
}

bool teardown_v2(engine_test_t* test) {
    (void)test;
    vals.clear();
    return true;
}

std::string get_dbname(const char* test_cfg) {
    std::string dbname;

    if (!test_cfg) {
        dbname.assign(dbname_env);
        return dbname;
    }

    const char *nm = strstr(test_cfg, "dbname=");
    if (nm == NULL) {
        dbname.assign(dbname_env);
    } else {
        dbname.assign(nm + 7);
        std::string::size_type end = dbname.find(';');
        if (end != dbname.npos) {
            dbname = dbname.substr(0, end);
        }
    }
    return dbname;
}

enum test_result prepare(engine_test_t *test) {
#ifdef __sun
        // Some of the tests doesn't work on Solaris.. Don't know why yet..
        if (strstr(test->name, "concurrent set") != NULL ||
            strstr(test->name, "retain rowid over a soft delete") != NULL)
        {
            return SKIPPED;
        }
#endif

    std::string dbname = get_dbname(test->cfg);
    /* Remove if the same DB directory already exists */
    try {
        rmdb(dbname.c_str());
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }
    cb::io::mkdirp(dbname);
    return SUCCESS;
}

enum test_result prepare_ep_bucket(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_rocks(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_magma(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_not_couchstore(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=couchstore") == std::string::npos) {
        return SKIPPED_UNDER_NOT_COUCHSTORE;
    }

    // Perform whatever prep the ep bucket function wants.
    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_rocks_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    if (cfg.find("backend=rocksdb") != std::string::npos &&
        cfg.find("item_eviction_policy=full_eviction")) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_magma_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    if (cfg.find("backend=magma") != std::string::npos &&
        cfg.find("item_eviction_policy=full_eviction")) {
        return SKIPPED_UNDER_MAGMA;
    }

    return prepare_ep_bucket(test);
}

enum test_result prepare_ep_bucket_skip_broken_under_not_couchstore_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") != std::string::npos) {
        return SKIPPED;
    }

    if (cfg.find("backend=couchstore") == std::string::npos &&
        cfg.find("item_eviction_policy=full_eviction")) {
        return SKIPPED_UNDER_NOT_COUCHSTORE;
    }

    return prepare_ep_bucket(test);
}

enum test_result prepare_skip_broken_under_rocks(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_magma(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_not_couchstore(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("backend=couchstore") == std::string::npos) {
        return SKIPPED_UNDER_NOT_COUCHSTORE;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_skip_broken_under_ephemeral_and_rocks(
        engine_test_t* test) {
    return prepare_ep_bucket_skip_broken_under_rocks(test);
}

enum test_result prepare_ephemeral_bucket(engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_full_eviction(engine_test_t *test) {

    // If we cannot find FE in the config, skip the test
    if (std::string(test->cfg).find("item_eviction_policy=full_eviction") ==
        std::string::npos) {
        return SKIPPED;
    }

    // Ephemeral buckets don't support full eviction.
    if (std::string(test->cfg).find("bucket_type=ephemeral")
            != std::string::npos) {
        return SKIPPED;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare(test);
}

enum test_result prepare_full_eviction_skip_under_rocks(engine_test_t *test) {

    std::string cfg{test->cfg};
    if (cfg.find("backend=rocksdb") != std::string::npos) {
        return SKIPPED_UNDER_ROCKSDB;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_full_eviction_skip_under_magma(engine_test_t *test) {

    std::string cfg{test->cfg};
    if (cfg.find("backend=magma") != std::string::npos) {
        return SKIPPED_UNDER_MAGMA;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_full_eviction_skip_under_not_couchstore(engine_test_t *test) {

    std::string cfg{test->cfg};
    if (cfg.find("backend=couchstore") == std::string::npos) {
        return SKIPPED_UNDER_NOT_COUCHSTORE;
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_skip_broken_under_rocks_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        if (cfg.find("backend=rocksdb") != std::string::npos &&
            cfg.find("item_eviction_policy=full_eviction")) {
            return SKIPPED_UNDER_ROCKSDB;
        }
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_skip_broken_under_magma_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        if (cfg.find("backend=magma") != std::string::npos &&
            cfg.find("item_eviction_policy=full_eviction")) {
            return SKIPPED_UNDER_MAGMA;
        }
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_skip_broken_under_not_couchstore_full_eviction(
        engine_test_t* test) {
    std::string cfg{test->cfg};
    if (cfg.find("bucket_type=ephemeral") == std::string::npos) {
        if (cfg.find("backend=couchstore") == std::string::npos &&
            cfg.find("item_eviction_policy=full_eviction")) {
            return SKIPPED_UNDER_NOT_COUCHSTORE;
        }
    }

    // Perform whatever prep the "base class" function wants.
    return prepare_full_eviction(test);
}

enum test_result prepare_skip_broken_under_ephemeral(engine_test_t *test) {
    return prepare_ep_bucket(test);
}

void cleanup(engine_test_t *test, enum test_result result) {
    (void)result;
    // Nuke the database files we created
    std::string dbname = get_dbname(test->cfg);
    /* Remove only the db file this test created */
    try {
        rmdb(dbname.c_str());
    } catch (std::system_error& e) {
        if (e.code() != std::error_code(ENOENT, std::system_category())) {
            throw e;
        }
    }
}

// Array of testcases to return back to engine_testapp.
static engine_test_t *testcases;

// Should only one test be run, and if so which number? If -1 then all tests
// are run.
static int oneTestIdx;

struct test_harness* testHarness;

// Array of testcases. Provided by the specific testsuite.
extern BaseTestCase testsuite_testcases[];

// Examines the list of tests provided by the specific testsuite
// via the testsuite_testcases[] array, populates `testcases` and returns it.
MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {

    // Calculate the size of the tests..
    int num = 0;
    while (testsuite_testcases[num].getName() != NULL) {
        ++num;
    }

    oneTestIdx = -1;
    char *testNum = getenv("EP_TEST_NUM");
    if (testNum) {
        sscanf(testNum, "%d", &oneTestIdx);
        if (oneTestIdx < 0 || oneTestIdx > num) {
            oneTestIdx = -1;
        }
    }
    dbname_env = getenv("EP_TEST_DIR");
    if (!dbname_env) {
        dbname_env = default_dbname;
    }

    if (oneTestIdx == -1) {
        testcases = static_cast<engine_test_t*>(cb_calloc(num + 1, sizeof(engine_test_t)));

        int ii = 0;
        for (int jj = 0; jj < num; ++jj) {
            engine_test_t *r = testsuite_testcases[jj].getTest();
            if (r != 0) {
                testcases[ii++] = *r;
            }
        }
    } else {
        testcases = static_cast<engine_test_t*>(cb_calloc(1 + 1, sizeof(engine_test_t)));

        engine_test_t *r = testsuite_testcases[oneTestIdx].getTest();
        if (r != 0) {
            testcases[0] = *r;
        }
    }

    return testcases;
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = th;
    return true;
}


MEMCACHED_PUBLIC_API
bool teardown_suite() {
    for (int i = 0; testcases[i].name != nullptr; i++) {
        cb_free((char*)testcases[i].name);
        cb_free((char*)testcases[i].cfg);
    }
    cb_free(testcases);
    testcases = NULL;
    return true;
}

/*
 * Create n_buckets and return how many were actually created.
 */
int create_buckets(const char* cfg, int n_buckets, std::vector<BucketHolder> &buckets) {
    std::string dbname = get_dbname(cfg);

    for (int ii = 0; ii < n_buckets; ii++) {
        std::stringstream config, dbpath;
        dbpath << dbname.c_str() << ii;
        std::string str_cfg(cfg);
        /* Find the position of "dbname=" in str_cfg */
        size_t pos = str_cfg.find("dbname=");
        if (pos != std::string::npos) {
            /* Move till end of the dbname */
            size_t new_pos = str_cfg.find(';', pos);
            str_cfg.insert(new_pos, std::to_string(ii));
            config << str_cfg;
        } else {
            config << str_cfg << "dbname=" << dbpath.str();
        }

        try {
            rmdb(dbpath.str().c_str());
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }
        EngineIface* handle =
                testHarness->create_bucket(true, config.str().c_str());
        if (handle) {
            buckets.push_back(BucketHolder(handle, dbpath.str()));
        } else {
            return ii;
        }
    }
    return n_buckets;
}

void destroy_buckets(std::vector<BucketHolder> &buckets) {
    for(auto bucket : buckets) {
        testHarness->destroy_bucket(bucket.h, false);
        rmdb(bucket.dbpath.c_str());
    }
}

void check_key_value(EngineIface* h,
                     const char* key,
                     const char* val,
                     size_t vlen,
                     Vbid vbucket) {
    // Fetch item itself, to ensure we maintain a ref-count on the underlying
    // Blob while comparing the key.
    auto getResult = get(h, NULL, key, vbucket);
    checkeq(cb::engine_errc::success,
            getResult.first,
            "Failed to fetch document");
    item_info info;
    check(h->get_item_info(getResult.second.get(), &info),
          "Failed to get_item_info");

    cb::const_char_buffer payload;
    cb::compression::Buffer inflated;
    if (isCompressionEnabled(h) &&
        (info.datatype & PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                 {static_cast<const char *>(info.value[0].iov_base),
                                  info.value[0].iov_len},
                                 inflated);
        payload = inflated;
    } else {
        payload = {static_cast<const char *>(info.value[0].iov_base),
                                             info.value[0].iov_len};
    }

    checkeq(cb::const_char_buffer(val, vlen), payload, "Data mismatch");
}

bool isCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() != BucketCompressionMode::Off;
}

bool isActiveCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() == BucketCompressionMode::Active;
}

bool isPassiveCompressionEnabled(EngineIface* h) {
    return h->getCompressionMode() == BucketCompressionMode::Passive;
}

bool isWarmupEnabled(EngineIface* h) {
    return get_bool_stat(h, "ep_warmup");
}

bool isPersistentBucket(EngineIface* h) {
    return get_str_stat(h, "ep_bucket_type") == "persistent";
}

bool isEphemeralBucket(EngineIface* h) {
    return get_str_stat(h, "ep_bucket_type") == "ephemeral";
}

void checkPersistentBucketTempItems(EngineIface* h, int exp) {
    if (isPersistentBucket(h)) {
        checkeq(exp,
                get_int_stat(h, "curr_temp_items"),
                "CheckPersistentBucketTempItems(): Num temp items not as "
                "expected");
    }
}

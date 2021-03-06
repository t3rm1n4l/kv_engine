CMAKE_MINIMUM_REQUIRED(VERSION 3.2)
PROJECT(EventuallyPersistentEngine LANGUAGES CXX)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckIncludeFileCXX)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckLibraryExists)
INCLUDE(CheckSymbolExists)
INCLUDE(CheckTypeSize)
INCLUDE(CMakeDependentOption)
INCLUDE(CTest)

CMAKE_DEPENDENT_OPTION(EP_USE_MAGMA "Enable support for Magma" ON
    "BUILD_ENTERPRISE" OFF)

CMAKE_DEPENDENT_OPTION(EP_USE_ROCKSDB "Enable support for RocksDB" ON
        "ROCKSDB_INCLUDE_DIR;ROCKSDB_LIBRARIES" OFF)

# The test in ep-engine is time consuming (and given that we run some of
# them with different modes it really adds up). By default we should build
# and run all of them, but in some cases it would be nice to be able to
# skip running them (for instance if one is working with the front-end and
# wants to speed up the test run)
CMAKE_DEPENDENT_OPTION(EP_BYPASS_TESTS "Bypass tests in ep-engine" OFF
                       "COUCHBASE_KV_BUILD_UNIT_TESTS" ON)

if (EP_BYPASS_TESTS)
   message(WARNING "Skipping tests in ep-engine")
endif()

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/src
                           ${CMAKE_CURRENT_BINARY_DIR}/src
                           ${BOOST_INCLUDE_DIR}
                           ${SNAPPY_INCLUDE_DIR}
                           ${BOOST_INCLUDE_DIR}
                           ${FLATBUFFERS_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${Platform_SOURCE_DIR}/external
                           ${Memcached_SOURCE_DIR}
                           ${Memcached_SOURCE_DIR}/utilities
                           ${Memcached_SOURCE_DIR}/tracing
                           ${Memcached_SOURCE_DIR}/include
                           ${Couchstore_SOURCE_DIR}/include
                           ${Couchstore_BINARY_DIR}/include
                           ${phosphor_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_BINARY_DIR})

# Mark third-party libraries as 'system' so we skip any warnings they
# generate.
INCLUDE_DIRECTORIES(BEFORE SYSTEM ${hdr_histogram_SOURCE_DIR}/src)

INCLUDE_DIRECTORIES(AFTER ${Memcached_BINARY_DIR}/include)

SET(EP_STORAGE_LIBS couchstore)

IF (EP_USE_ROCKSDB)
    INCLUDE_DIRECTORIES(AFTER ${ROCKSDB_INCLUDE_DIR})
    SET(ROCKSDB_KVSTORE_SOURCE src/rocksdb-kvstore/rocksdb-kvstore.cc
        src/rocksdb-kvstore/rocksdb-kvstore_config.cc)
    LIST(APPEND EP_STORAGE_LIBS ${ROCKSDB_LIBRARIES})
    ADD_DEFINITIONS(-DEP_USE_ROCKSDB=1)
    MESSAGE(STATUS "ep-engine: Using RocksDB")
ENDIF (EP_USE_ROCKSDB)

IF (EP_USE_MAGMA)
    IF (EXISTS ${MAGMA_INCLUDE_DIR})
        INCLUDE_DIRECTORIES(AFTER ${MAGMA_INCLUDE_DIR})
        LIST(APPEND EP_STORAGE_LIBS magma)
        MESSAGE(STATUS "ep-engine: Found Magma include:" ${MAGMA_INCLUDE_DIR})
    ENDIF (EXISTS ${MAGMA_INCLUDE_DIR})
    SET(MAGMA_KVSTORE_SOURCE src/magma-kvstore/magma-kvstore.cc
                             src/magma-kvstore/magma-kvstore_config.cc)
    ADD_DEFINITIONS(-DEP_USE_MAGMA=1)
    MESSAGE(STATUS "ep-engine: Building magma-kvstore")
ENDIF (EP_USE_MAGMA)

INCLUDE_DIRECTORIES(AFTER SYSTEM
                    ${gtest_SOURCE_DIR}/include
                    ${gmock_SOURCE_DIR}/include)

CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("netdb.h" HAVE_NETDB_H)
CHECK_INCLUDE_FILES("poll.h" HAVE_POLL_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("sys/socket.h" HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILES("sys/time.h" HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES("netinet/in.h" HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILES("netinet/tcp.h" HAVE_NETINET_TCP_H)

# For debugging without compiler optimizations uncomment line below..
#SET (CMAKE_BUILD_TYPE DEBUG)

SET(EP_ENGINE_VERSION "${MEMCACHED_VERSION}")

CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/src/config.cmake.h
                ${CMAKE_CURRENT_BINARY_DIR}/src/config.h)

# Generate the python wrappers
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbepctl)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbstats)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbcompact)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvdiff)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvbucketctl)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/kv_trace_dump)

SET(Python_ADDITIONAL_VERSIONS 2.6)
FIND_PACKAGE(PythonInterp)

# to avoid cluttering source dir with name + "c" files
SET(ENV{PYTHONDONTWRITEBYTECODE} 1)

if (NOT EP_BYPASS_TESTS)
    # runs cbstats & cbepctl - will fail on basic syntax errors
    ADD_TEST(NAME cbstats_test
             COMMAND ${PYTHON_EXECUTABLE}
             ${CMAKE_CURRENT_SOURCE_DIR}/management/cbstats --help)
    ADD_TEST(NAME cbepctl_test
             COMMAND ${PYTHON_EXECUTABLE}
             ${CMAKE_CURRENT_SOURCE_DIR}/management/cbepctl --help)
    ADD_TEST(NAME kv_trace_dump_test
             COMMAND ${PYTHON_EXECUTABLE}
             ${CMAKE_CURRENT_SOURCE_DIR}/management/kv_trace_dump --help)
endif (NOT EP_BYPASS_TESTS)

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_SOURCE_DIR}/platform/include/win32)
ENDIF (WIN32)

ADD_EXECUTABLE(gencode tools/gencode.cc)
TARGET_LINK_LIBRARIES(gencode cJSON platform)

ADD_EXECUTABLE(genconfig tools/genconfig.cc)
TARGET_LINK_LIBRARIES(genconfig cJSON platform)

add_subdirectory(tools/couchfile_upgrade)
include_directories(tools/couchfile_upgrade)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
                          ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h
                  COMMAND
                     gencode -j ${CMAKE_CURRENT_SOURCE_DIR}/docs/stats.json -h ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h -c ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c -f get_stats_info
                  DEPENDS
                        docs/stats.json
                        gencode
                  COMMENT "Generating code for stats info")

ADD_CUSTOM_COMMAND(OUTPUT
                     src/generated_configuration.cc.temp
                     src/generated_configuration.h.temp
                  COMMAND
                     genconfig ${CMAKE_CURRENT_SOURCE_DIR}/configuration.json src/generated_configuration.h.temp src/generated_configuration.cc.temp
                  DEPENDS
                        configuration.json
                        genconfig
                  COMMENT "Generating code for configuration class")

ADD_CUSTOM_COMMAND(OUTPUT
                     src/generated_configuration.cc
                     src/generated_configuration.h
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.cc.temp src/generated_configuration.cc
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.h.temp src/generated_configuration.h
                  DEPENDS
                     src/generated_configuration.cc.temp
                     src/generated_configuration.h.temp
                  COMMENT "Copying code for configuration class")


ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/collections/vbucket_serialised_manifest_entry_generated.h
                  COMMAND
                     ${FLATC} -o ${CMAKE_CURRENT_BINARY_DIR}/src/collections/ --gen-mutable --cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/collections/vbucket_serialised_manifest_entry.fbs
                  DEPENDS
                     src/collections/vbucket_serialised_manifest_entry.fbs
                  COMMENT "Generating flatbuffers serialied_manifest_entry_generated")

SET(COUCH_KVSTORE_SOURCE src/couch-kvstore/couch-kvstore.cc
            src/couch-kvstore/couch-fs-stats.cc)
SET(OBJECTREGISTRY_SOURCE src/objectregistry.cc)
SET(CONFIG_SOURCE src/configuration.cc
  ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc)

SET(COLLECTIONS_SOURCE src/collections/collections_callbacks.cc
                       src/collections/collections_types.cc
                       src/collections/flush.cc
                       src/collections/manager.cc
                       src/collections/manifest.cc
                       ${CMAKE_CURRENT_BINARY_DIR}/src/collections/vbucket_serialised_manifest_entry_generated.h
                       src/collections/vbucket_filter.cc
                       src/collections/vbucket_manifest.cc
                       src/collections/vbucket_manifest_entry.cc)

ADD_LIBRARY(ep_objs OBJECT
            src/access_scanner.cc
            src/atomic.cc
            src/bgfetcher.cc
            src/blob.cc
            src/bloomfilter.cc
            src/bucket_logger.cc
            src/callbacks.cc
            src/checkpoint.cc
            src/checkpoint_config.cc
            src/checkpoint_manager.cc
            src/checkpoint_remover.cc
            src/conflict_resolution.cc
            src/connhandler.cc
            src/connmap.cc
            src/crc32.c
            src/dcp/active_stream.cc
            src/dcp/active_stream.h
            src/dcp/active_stream_checkpoint_processor_task.cc
            src/dcp/active_stream_checkpoint_processor_task.h
            src/dcp/backfill.cc
            src/dcp/backfill-manager.cc
            src/dcp/backfill_disk.cc
            src/dcp/backfill_memory.cc
            src/dcp/consumer.cc
            src/dcp/dcp-types.h
            src/dcp/dcpconnmap.cc
            src/dcp/flow-control.cc
            src/dcp/flow-control-manager.cc
            src/dcp/notifier_stream.cc
            src/dcp/notifier_stream.h
            src/dcp/passive_stream.cc
            src/dcp/passive_stream.h
            src/dcp/producer.cc
            src/dcp/ready-queue.cc
            src/dcp/ready-queue.h
            src/dcp/response.cc
            src/dcp/stream.cc
            src/defragmenter.cc
            src/defragmenter_visitor.cc
            src/ep_bucket.cc
            src/ep_vb.cc
            src/ep_engine.cc
            src/ep_time.cc
            src/ep_types.cc
            src/ephemeral_bucket.cc
            src/ephemeral_tombstone_purger.cc
            src/ephemeral_vb.cc
            src/ephemeral_vb_count_visitor.cc
            src/executorpool.cc
            src/executorthread.cc
            src/ext_meta_parser.cc
            src/failover-table.cc
            src/flusher.cc
            src/globaltask.cc
            src/hash_table.cc
            src/hdrhistogram.cc
            src/hlc.cc
            src/htresizer.cc
            src/item.cc
            src/item_compressor.cc
            src/item_compressor_visitor.cc
            src/item_eviction.cc
            src/item_freq_decayer.cc
            src/item_freq_decayer_visitor.cc
            src/item_pager.cc
            src/kvstore.cc
            src/kvstore_config.cc
            src/kv_bucket.cc
            src/kvshard.cc
            src/memory_tracker.cc
            src/murmurhash3.cc
            src/mutation_log.cc
            src/mutation_log_entry.cc
            src/paging_visitor.cc
            src/persistence_callback.cc
            src/pre_link_document_context.cc
            src/pre_link_document_context.h
            src/progress_tracker.cc
            src/replicationthrottle.cc
            src/linked_list.cc
            src/seqlist.cc
            src/stats.cc
            src/string_utils.cc
            src/storeddockey.cc
            src/stored-value.cc
            src/stored_value_factories.cc
            src/stored_value_factories.h
            src/systemevent.cc
            src/tasks.cc
            src/taskqueue.cc
            src/vb_count_visitor.cc
            src/vb_visitors.cc
            src/vbucket.cc
            src/vbucketmap.cc
            src/vbucketdeletiontask.cc
            src/warmup.cc
            ${OBJECTREGISTRY_SOURCE}
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${CONFIG_SOURCE}
            ${COUCH_KVSTORE_SOURCE}
            ${ROCKSDB_KVSTORE_SOURCE}
            ${MAGMA_KVSTORE_SOURCE}
            ${COLLECTIONS_SOURCE})
SET_PROPERTY(TARGET ep_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(ep_objs)

ADD_LIBRARY(ep SHARED $<TARGET_OBJECTS:ep_objs>)

SET_TARGET_PROPERTIES(ep PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep cJSON JSON_checker ${EP_STORAGE_LIBS}
                      engine_utilities dirutils cbcompress hdr_histogram_static
                      mcd_util platform phosphor xattr mcd_tracing
                      ${LIBEVENT_LIBRARIES})
add_sanitizers(ep)

ADD_LIBRARY(mock_dcp OBJECT tests/mock/mock_dcp.cc)
SET_PROPERTY(TARGET mock_dcp PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(mock_dcp)

ADD_LIBRARY(ep_testsuite_objs OBJECT
            tests/ep_testsuite_common.cc
            tests/ep_test_apis.cc
            src/ext_meta_parser.cc
            src/storeddockey.cc)
SET_PROPERTY(TARGET ep_testsuite_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(ep_testsuite_objs)

if (NOT EP_BYPASS_TESTS)
    # Single executable containing all class-level unit tests involving
    # EventuallyPersistentEngine driven by GoogleTest.
    # (We end up compiling most of the src/ files of ep-engine for these unit tests,
    # so simpler / quicker just to link them into a single executable).
    ADD_EXECUTABLE(ep-engine_ep_unit_tests
                   tests/mock/mock_dcp_producer.cc
                   tests/mock/mock_ephemeral_vb.cc
                   tests/mock/mock_stream.cc
                   tests/mock/mock_synchronous_ep_engine.cc
                   tests/module_tests/atomic_unordered_map_test.cc
                   tests/module_tests/basic_ll_test.cc
                   tests/module_tests/bloomfilter_test.cc
                   tests/module_tests/bucket_logger_engine_test.cc
                   tests/module_tests/bucket_logger_test.cc
                   tests/module_tests/checkpoint_remover_test.h
                   tests/module_tests/checkpoint_remover_test.cc
                   tests/module_tests/checkpoint_test.h
                   tests/module_tests/checkpoint_test.cc
                   tests/module_tests/checkpoint_utils.h
                   tests/module_tests/collections/collections_dcp_test.cc
                   tests/module_tests/collections/evp_store_collections_dcp_test.cc
                   tests/module_tests/collections/evp_store_collections_eraser_test.cc
                   tests/module_tests/collections/evp_store_collections_test.cc
                   tests/module_tests/collections/filter_test.cc
                   tests/module_tests/collections/manifest_test.cc
                   tests/module_tests/collections/test_manifest.cc
                   tests/module_tests/collections/vbucket_manifest_test.cc
                   tests/module_tests/collections/vbucket_manifest_entry_test.cc
                   tests/module_tests/configuration_test.cc
                   tests/module_tests/defragmenter_test.cc
                   tests/module_tests/dcp_reflection_test.cc
                   tests/module_tests/dcp_test.cc
                   tests/module_tests/dcp_utils.cc
                   tests/module_tests/ep_unit_tests_main.cc
                   tests/module_tests/ephemeral_bucket_test.cc
                   tests/module_tests/ephemeral_vb_test.cc
                   tests/module_tests/evp_engine_test.cc
                   tests/module_tests/evp_store_rollback_test.cc
                   tests/module_tests/evp_store_test.cc
                   tests/module_tests/evp_store_single_threaded_test.cc
                   tests/module_tests/evp_store_with_meta.cc
                   tests/module_tests/executorpool_test.cc
                   tests/module_tests/failover_table_test.cc
                   tests/module_tests/futurequeue_test.cc
                   tests/module_tests/hash_table_eviction_test.cc
                   tests/module_tests/hash_table_test.cc
                   tests/module_tests/hdrhistogram_test.cc
                   tests/module_tests/item_compressor_test.cc
                   tests/module_tests/item_eviction_test.cc
                   tests/module_tests/item_pager_test.cc
                   tests/module_tests/item_test.cc
                   tests/module_tests/kvstore_test.cc
                   tests/module_tests/kv_bucket_test.cc
                   tests/module_tests/memory_tracker_test.cc
                   tests/module_tests/mock_hooks_api.cc
                   tests/module_tests/monotonic_test.cc
                   tests/module_tests/mutation_log_test.cc
                   tests/module_tests/objectregistry_test.cc
                   tests/module_tests/mutex_test.cc
                   tests/module_tests/probabilistic_counter_test.cc
                   tests/module_tests/stats_test.cc
                   tests/module_tests/storeddockey_test.cc
                   tests/module_tests/stored_value_test.cc
                   tests/module_tests/systemevent_test.cc
                   tests/module_tests/tagged_ptr_test.cc
                   tests/module_tests/test_helpers.cc
                   tests/module_tests/vbucket_test.cc
                   tests/module_tests/warmup_test.cc
                   $<TARGET_OBJECTS:mock_dcp>
                   $<TARGET_OBJECTS:ep_objs>
                   $<TARGET_OBJECTS:memory_tracking>
                   $<TARGET_OBJECTS:couchstore_test_fileops>
                   $<TARGET_OBJECTS:couchfile_upgrade_objects>
                   ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
                   ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
                   ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
                   ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)

    TARGET_INCLUDE_DIRECTORIES(ep-engine_ep_unit_tests
                               PUBLIC
                               ${Couchstore_SOURCE_DIR})

    TARGET_LINK_LIBRARIES(ep-engine_ep_unit_tests ${EP_STORAGE_LIBS} cJSON
                          dirutils engine_utilities gtest gmock hdr_histogram_static
                          JSON_checker memcached_logger mcbp mcd_util mcd_tracing platform
                          phosphor xattr cbcompress ${MALLOC_LIBRARIES})
    add_sanitizers(ep-engine_ep_unit_tests)

    ADD_EXECUTABLE(ep-engine_atomic_ptr_test
                   tests/module_tests/atomic_ptr_test.cc
                   src/atomic.cc
            )
    TARGET_LINK_LIBRARIES(ep-engine_atomic_ptr_test platform)

    ADD_EXECUTABLE(ep-engine_couch-fs-stats_test
                   src/couch-kvstore/couch-fs-stats.cc
                   src/generated_configuration.h
                   tests/module_tests/couch-fs-stats_test.cc
                   $<TARGET_OBJECTS:couchstore_wrapped_fileops_test_framework>)
    TARGET_INCLUDE_DIRECTORIES(ep-engine_couch-fs-stats_test
                               PRIVATE
                               ${Couchstore_SOURCE_DIR}
                               ${Couchstore_SOURCE_DIR}/src)
    TARGET_LINK_LIBRARIES(ep-engine_couch-fs-stats_test gtest gtest_main gmock platform)

    ADD_EXECUTABLE(ep-engine_misc_test tests/module_tests/misc_test.cc)
    TARGET_LINK_LIBRARIES(ep-engine_misc_test mcbp platform)

    ADD_EXECUTABLE(ep-engine_string_utils_test
                   tests/module_tests/string_utils_test.cc
                   src/string_utils.cc)
    TARGET_LINK_LIBRARIES(ep-engine_string_utils_test gtest gtest_main platform)

    ADD_EXECUTABLE(ep_engine_benchmarks
                   benchmarks/access_scanner_bench.cc
                   benchmarks/benchmark_memory_tracker.cc
                   benchmarks/defragmenter_bench.cc
                   benchmarks/engine_fixture.cc
                   benchmarks/ep_engine_benchmarks_main.cc
                   benchmarks/hash_table_bench.cc
                   benchmarks/item_bench.cc
                   benchmarks/item_compressor_bench.cc
                   benchmarks/kvstore_bench.cc
                   benchmarks/mem_allocator_stats_bench.cc
                   benchmarks/vbucket_bench.cc
                   benchmarks/probabilistic_counter_bench.cc
                   tests/mock/mock_synchronous_ep_engine.cc
                   $<TARGET_OBJECTS:ep_objs>
                   $<TARGET_OBJECTS:memory_tracking>
                   $<TARGET_OBJECTS:couchstore_test_fileops>
                   ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
                   ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
                   ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
                   ${Memcached_SOURCE_DIR}/engines/ep/tests/module_tests/test_helpers.cc)

    TARGET_LINK_LIBRARIES(ep_engine_benchmarks PRIVATE
            benchmark
            cbcompress
            cJSON
            dirutils
            engine_utilities
            gtest
            gmock
            hdr_histogram_static
            JSON_checker
            mcd_util
            mcd_tracing
            memcached_logger
            phosphor
            platform
            xattr
            ${MALLOC_LIBRARIES}
            ${EP_STORAGE_LIBS})
    TARGET_INCLUDE_DIRECTORIES(ep_engine_benchmarks PUBLIC
                               ${benchmark_SOURCE_DIR}/include
                               tests
                               benchmarks
                               ${Couchstore_SOURCE_DIR})
    add_sanitizers(ep_engine_benchmarks)

    ADD_TEST(NAME ep-engine_atomic_ptr_test COMMAND ep-engine_atomic_ptr_test)
    ADD_TEST(NAME ep-engine_couch-fs-stats_test COMMAND ep-engine_couch-fs-stats_test)
    ADD_TEST(NAME ep-engine_ep_unit_tests COMMAND ep-engine_ep_unit_tests)
    ADD_TEST(NAME ep-engine_misc_test COMMAND ep-engine_misc_test)

    ADD_LIBRARY(timing_tests SHARED tests/module_tests/timing_tests.cc)
    SET_TARGET_PROPERTIES(timing_tests PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(timing_tests mcbp platform)

    ADD_EXECUTABLE(ep-engine_sizes src/sizes.cc
                   $<TARGET_OBJECTS:ep_objs>)
    TARGET_LINK_LIBRARIES(ep-engine_sizes cJSON JSON_checker hdr_histogram_static
                          engine_utilities ${EP_STORAGE_LIBS} dirutils cbcompress platform mcd_util
                          mcd_tracing phosphor xattr ${LIBEVENT_LIBRARIES})
    add_sanitizers(ep-engine_sizes)

    ADD_LIBRARY(ep_testsuite SHARED
                tests/ep_testsuite.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>
                $<TARGET_OBJECTS:mock_dcp>)
    SET_TARGET_PROPERTIES(ep_testsuite PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_testsuite engine_utilities mcbp mcd_util
                          ${EP_STORAGE_LIBS} cbcompress dirutils JSON_checker
                          platform xattr
                          ${LIBEVENT_LIBRARIES})
    ADD_DEPENDENCIES(ep_testsuite engine_testapp)

    ADD_LIBRARY(ep_testsuite_basic SHARED
                tests/ep_testsuite_basic.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>
                $<TARGET_OBJECTS:mock_dcp>
                )
    SET_TARGET_PROPERTIES(ep_testsuite_basic PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_testsuite_basic
            cbcompress
            dirutils
            engine_utilities
            JSON_checker
            mcbp
            mcd_util
            platform
            ${LIBEVENT_LIBRARIES}
            ${SNAPPY_LIBRARIES})
    ADD_DEPENDENCIES(ep_testsuite engine_testapp)

    ADD_LIBRARY(ep_testsuite_dcp SHARED
                tests/ep_testsuite_dcp.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>
                $<TARGET_OBJECTS:mock_dcp>
                )
    SET_TARGET_PROPERTIES(ep_testsuite_dcp PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_testsuite_dcp
            cbcompress
            dirutils
            engine_utilities
            JSON_checker
            mcbp
            mcd_util
            platform
            ${LIBEVENT_LIBRARIES}
            ${SNAPPY_LIBRARIES})
    ADD_DEPENDENCIES(ep_testsuite_dcp engine_testapp)

    ADD_LIBRARY(ep_testsuite_checkpoint SHARED
                tests/ep_testsuite_checkpoint.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>)
    SET_TARGET_PROPERTIES(ep_testsuite_checkpoint PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_testsuite_checkpoint
            cbcompress
            dirutils
            engine_utilities
            JSON_checker
            mcbp
            mcd_util
            platform
            ${LIBEVENT_LIBRARIES}
            ${SNAPPY_LIBRARIES})
    ADD_DEPENDENCIES(ep_testsuite_checkpoint engine_testapp)

    ADD_LIBRARY(ep_testsuite_xdcr SHARED
                tests/ep_testsuite_xdcr.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>)
    SET_TARGET_PROPERTIES(ep_testsuite_xdcr PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_testsuite_xdcr mcd_util engine_utilities mcbp JSON_checker dirutils platform xattr
                          ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
    ADD_DEPENDENCIES(ep_testsuite_xdcr engine_testapp)

    ADD_LIBRARY(ep_perfsuite SHARED
                tests/ep_perfsuite.cc
                $<TARGET_OBJECTS:ep_testsuite_objs>
                $<TARGET_OBJECTS:mock_dcp>)
    SET_TARGET_PROPERTIES(ep_perfsuite PROPERTIES PREFIX "")
    TARGET_LINK_LIBRARIES(ep_perfsuite
            cbcompress
            dirutils
            engine_utilities
            mcbp
            mcd_util
            platform)
    ADD_DEPENDENCIES(ep_perfsuite engine_testapp)

    #ADD_CUSTOM_COMMAND(OUTPUT
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_0.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_1.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_2.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_3.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_4.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_5.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_6.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_7.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_8.c
    #                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_9.c
    #                  COMMAND
    #                     ${PYTHON_EXECUTABLE} tests/module_tests/gen_engine_test.py
    #                  DEPENDS
    #                        tests/module_tests/gen_engine_test.py
    #                        tests/module_tests/breakdancer.py
    #                  COMMENT "Generating testsuite")
    #
    #ADD_LIBRARY(generated_testsuite SHARED
    #            tests/suite_stubs.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_0.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_1.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_2.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_3.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_4.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_5.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_6.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_7.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_8.c
    #            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_9.c)

    #SET_TARGET_PROPERTIES(generated_testsuite PROPERTIES PREFIX "")
endif (NOT EP_BYPASS_TESTS)

INSTALL(PROGRAMS
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbepctl
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbstats
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbcompact
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvdiff
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvbucketctl
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/kv_trace_dump
        management/cbanalyze-core
        DESTINATION bin)

INSTALL(PROGRAMS
        management/cbepctl
        management/cbstats
        management/cbcompact
        management/cbvdiff
        management/cbvbucketctl
        management/kv_trace_dump
        DESTINATION lib/python)

INSTALL(FILES
        management/clitool.py
        management/cli_auth_utils.py
        management/mc_bin_client.py
        management/mc_bin_server.py
        management/memcacheConstants.py
        DESTINATION lib/python)

INSTALL(FILES
        docs/stats.org
        DESTINATION share/doc/ep-engine)

INSTALL(TARGETS ep
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

if (NOT EP_BYPASS_TESTS)
    IF (EP_USE_ROCKSDB)
        ADD_TEST(NAME ep-engine-persistence-unit-tests.rocksdb
                 COMMAND memcached_testapp -E ep -c "backend=rocksdb$<SEMICOLON>collections_enabled=false" --gtest_filter=*PersistToTest*)
        SET_TESTS_PROPERTIES(ep-engine-persistence-unit-tests.rocksdb PROPERTIES TIMEOUT 300)
    ENDIF (EP_USE_ROCKSDB)

    # Defines a testsuite which runs in full and value eviction
    # (persistent), ephemeral buckets (which has no eviction).
    FUNCTION(ADD_TESTSUITE name timeout)
        CMAKE_PARSE_ARGUMENTS("arg" "SKIP_EPHEMERAL" "" "" ${ARGN})

        SET(_cmdline
            ${Memcached_BINARY_DIR}/engine_testapp
            -E $<TARGET_FILE:ep>
            -T $<TARGET_FILE:${name}>)
        ADD_TEST(NAME ${name}.value_eviction.comp_passive
                 COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>dbname=./${name}.value_eviction.comp_passive.db")
        ADD_TEST(NAME ${name}.value_eviction.comp_active
                 COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>dbname=./${name}.value_eviction.comp_active.db")
        ADD_TEST(NAME ${name}.full_eviction.comp_passive
                 COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>item_eviction_policy=full_eviction\
$<SEMICOLON>dbname=./${name}.full_eviction.comp_passive.db")
        ADD_TEST(NAME ${name}.full_eviction.comp_active
                 COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>item_eviction_policy=full_eviction\
$<SEMICOLON>dbname=./${name}.full_eviction.comp_active.db")
        SET_TESTS_PROPERTIES(${name}.value_eviction.comp_passive PROPERTIES TIMEOUT ${timeout})
        SET_TESTS_PROPERTIES(${name}.value_eviction.comp_active PROPERTIES TIMEOUT ${timeout})
        SET_TESTS_PROPERTIES(${name}.full_eviction.comp_passive PROPERTIES TIMEOUT ${timeout})
        SET_TESTS_PROPERTIES(${name}.full_eviction.comp_active PROPERTIES TIMEOUT ${timeout})

        IF (NOT arg_SKIP_EPHEMERAL)
            ADD_TEST(NAME ${name}.ephemeral.comp_passive
                     COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_passive.db")
            ADD_TEST(NAME ${name}.ephemeral.comp_active
                     COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_active.db")
            SET_TESTS_PROPERTIES(${name}.ephemeral.comp_passive PROPERTIES TIMEOUT ${timeout})
            SET_TESTS_PROPERTIES(${name}.ephemeral.comp_active PROPERTIES TIMEOUT ${timeout})
        ENDIF ()

        IF (EP_USE_ROCKSDB)
            ADD_TEST(NAME ${name}.value_eviction.rocksdb
                     COMMAND ${_cmdline} -v -e "dbname=./${name}.value_eviction.rocksdb$<SEMICOLON>backend=rocksdb$<SEMICOLON>collections_enabled=false")
            ADD_TEST(NAME ${name}.full_eviction.rocksdb
                     COMMAND ${_cmdline} -v -e "item_eviction_policy=full_eviction$<SEMICOLON>dbname=./${name}.full_eviction.rocksdb$<SEMICOLON>backend=rocksdb$<SEMICOLON>collections_enabled=false")
            SET_TESTS_PROPERTIES(${name}.value_eviction.rocksdb PROPERTIES TIMEOUT ${timeout})
            SET_TESTS_PROPERTIES(${name}.full_eviction.rocksdb PROPERTIES TIMEOUT ${timeout})
        ENDIF (EP_USE_ROCKSDB)
    ENDFUNCTION()

    ADD_TESTSUITE(ep_testsuite 1800)

    ADD_TESTSUITE(ep_testsuite_basic 600)

    ADD_TESTSUITE(ep_testsuite_dcp 1200)

    ADD_TESTSUITE(ep_testsuite_checkpoint 120)

    ADD_TESTSUITE(ep_testsuite_xdcr 120)

    # ================================ PERF_TESTS ================================ #
    SET(_ep_perfsuite_cmdline $<TARGET_FILE:engine_testapp> -E $<TARGET_FILE:ep> -T $<TARGET_FILE:ep_perfsuite> -v)

    # Micro perf tests: value-only eviction mode. Note that it makes no sense to run
    # these tests in full-eviction mode as we disable persistence - see MB-19504.
    ADD_CUSTOM_TARGET(test-perfsuite
                      COMMAND ${_ep_perfsuite_cmdline}
                      DEPENDS engine_testapp
                      ep
                      ep_perfsuite
                      VERBATIM)
    ADD_TEST(NAME ep_perfsuite.value_eviction
             COMMAND ${_ep_perfsuite_cmdline} -e "dbname=./ep_perfsuite.value_eviction.db")

    ADD_TEST(NAME ep_perfsuite.ephemeral
             COMMAND ${_ep_perfsuite_cmdline} -e "bucket_type=ephemeral$<SEMICOLON>dbname=./ep_perfsuite.ephemeral.db")

    # ============================================================================ #

    ENABLE_CODE_COVERAGE_REPORT()
endif (NOT EP_BYPASS_TESTS)


# Extra (non-default) target to compile all ep-engine headers (for
# build profiling / checking for correct #includes).
file(GLOB_RECURSE ep_headers
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  CONFIGURE_DEPENDS
  *.h)
# List of headers to ignore / not attempt to compile
list(REMOVE_ITEM ep_headers
  src/config.cmake.h  # XXXX
  src/tasks.def.h  # X-Macro file; cannot be compiled by itself
  src/threadlocal_posix.h  # POSIX specific code; included via threadlocal.h
  src/threadlocal_win32.h  # Win32 specific code; included via threadlocal.h
)

add_header_object_library(NAME ep_headers_obj HEADERS ${ep_headers})
target_include_directories(ep_headers_obj PUBLIC
                           ${benchmark_SOURCE_DIR}/include)

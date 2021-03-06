#ifndef MEMCACHED_MOCK_SERVER_H
#define MEMCACHED_MOCK_SERVER_H

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_callback_iface.h>
#include <platform/platform.h>

#include <atomic>
#include <string>

#include "tracing/tracer.h"

struct mock_connstruct : cb::tracing::Traceable {
    mock_connstruct();

    uint64_t magic;
    std::string uname;
    void *engine_data;
    bool connected;
    int sfd;
    ENGINE_ERROR_CODE status;
    int nblocks; /* number of ewouldblocks */
    bool handle_ewouldblock;
    bool handle_mutation_extras;
    std::bitset<8> enabled_datatypes;
    bool handle_collections_support;
    cb_mutex_t mutex;
    cb_cond_t cond;
    int references;
    uint64_t num_io_notifications;
    uint64_t num_processed_notifications;

    ~mock_connstruct();
};

struct mock_callbacks {
    EVENT_CALLBACK cb;
    const void *cb_data;
};

struct mock_stats {
    uint64_t astat;
};

MEMCACHED_PUBLIC_API void mock_init_alloc_hooks(void);

MEMCACHED_PUBLIC_API SERVER_HANDLE_V1 *get_mock_server_api(void);

MEMCACHED_PUBLIC_API void init_mock_server();

MEMCACHED_PUBLIC_API const void *create_mock_cookie(void);

MEMCACHED_PUBLIC_API void destroy_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void mock_set_ewouldblock_handling(const void *cookie, bool enable);

MEMCACHED_PUBLIC_API void mock_set_mutation_extras_handling(const void *cookie,
                                                            bool enable);

MEMCACHED_PUBLIC_API void mock_set_collections_support(const void *cookie,
                                                       bool enable);

MEMCACHED_PUBLIC_API void mock_set_datatype_support(
        const void* cookie, protocol_binary_datatype_t datatypes);

MEMCACHED_PUBLIC_API void lock_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void unlock_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void waitfor_mock_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void mock_time_travel(int by);

MEMCACHED_PUBLIC_API void disconnect_all_mock_connections(void);

MEMCACHED_PUBLIC_API void destroy_mock_event_callbacks(void);

MEMCACHED_PUBLIC_API int get_number_of_mock_cookie_references(const void *cookie);

MEMCACHED_PUBLIC_API size_t
get_number_of_mock_cookie_io_notifications(const void* cookie);

void mock_set_pre_link_function(PreLinkFunction function);

MEMCACHED_PUBLIC_API cb::tracing::Traceable& mock_get_traceable(
        const void* cookie);

#endif  /* MEMCACHED_MOCK_SERVER_H */

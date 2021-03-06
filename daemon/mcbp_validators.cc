/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "config.h"

#include "buckets.h"
#include "connection.h"
#include "cookie.h"
#include "mcbp_validators.h"
#include "memcached.h"
#include "subdocument_validators.h"
#include "xattr/utils.h"
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
#include <platform/string_hex.h>

using Status = cb::mcbp::Status;

static inline bool may_accept_xattr(const Cookie& cookie) {
    auto* req = static_cast<protocol_binary_request_header*>(
            cookie.getPacketAsVoidPtr());
    if (mcbp::datatype::is_xattr(req->request.datatype)) {
        return cookie.getConnection().isXattrEnabled();
    }

    return true;
}

bool is_document_key_valid(const Cookie& cookie) {
    const auto& req = cookie.getRequest(Cookie::PacketContent::Header);
    if (cookie.getConnection().isCollectionsSupported()) {
        const auto& key = req.getKey();
        auto stopByte = cb::mcbp::unsigned_leb128_get_stop_byte_index(key);
        // 1. CID is leb128 encode, key must then be 1 byte of key and 1 byte of
        //    leb128 minimum
        // 2. Secondly - require that the leb128 and key are encoded, i.e. we
        //    expect that the leb128 stop byte is not the last byte of the key.
        return req.getKeylen() > 1 && stopByte && (key.size() - 1) > *stopByte;
    }
    return req.getKeylen() > 0;
}

static inline bool may_accept_dcp_deleteV2(const Cookie& cookie) {
    return cookie.getConnection().isDcpDeleteV2();
}

static inline std::string get_peer_description(const Cookie& cookie) {
    return cookie.getConnection().getDescription();
}

enum class ExpectedKeyLen { Zero, NonZero, Any };
enum class ExpectedValueLen { Zero, NonZero, Any };
enum class ExpectedCas { Set, NotSet, Any };

/**
 * Verify the header meets basic sanity checks and fields length
 * match the provided expected lengths.
 */
static bool verify_header(
        Cookie& cookie,
        uint8_t expected_extlen,
        ExpectedKeyLen expected_keylen,
        ExpectedValueLen expected_valuelen,
        ExpectedCas expected_cas = ExpectedCas::Any,
        uint8_t expected_datatype_mask = mcbp::datatype::highest) {
    const auto& header = cookie.getHeader();

    if (!header.isValid()) {
        cookie.setErrorContext("Request header invalid");
        return false;
    }
    if (!mcbp::datatype::is_valid(header.getDatatype())) {
        cookie.setErrorContext("Request datatype invalid");
        return false;
    }

    if ((expected_extlen == 0) && (header.getExtlen() != 0)) {
        cookie.setErrorContext("Request must not include extras");
        return false;
    }
    if ((expected_extlen != 0) && (header.getExtlen() != expected_extlen)) {
        cookie.setErrorContext("Request must include extras of length " +
                               std::to_string(expected_extlen));
        return false;
    }

    switch (expected_keylen) {
    case ExpectedKeyLen::Zero:
        if (header.getKeylen() != 0) {
            cookie.setErrorContext("Request must not include key");
            return false;
        }
        break;
    case ExpectedKeyLen::NonZero:
        if (header.getKeylen() == 0) {
            cookie.setErrorContext("Request must include key");
            return false;
        }
        break;
    case ExpectedKeyLen::Any:
        break;
    }

    uint32_t valuelen =
            header.getBodylen() - header.getKeylen() - header.getExtlen();
    switch (expected_valuelen) {
    case ExpectedValueLen::Zero:
        if (valuelen != 0) {
            cookie.setErrorContext("Request must not include value");
            return false;
        }
        break;
    case ExpectedValueLen::NonZero:
        if (valuelen == 0) {
            cookie.setErrorContext("Request must include value");
            return false;
        }
        break;
    case ExpectedValueLen::Any:
        break;
    }

    switch (expected_cas) {
    case ExpectedCas::NotSet:
        if (header.getCas() != 0) {
            cookie.setErrorContext("Request CAS must not be set");
            return false;
        }
        break;
    case ExpectedCas::Set:
        if (header.getCas() == 0) {
            cookie.setErrorContext("Request CAS must be set");
            return false;
        }
        break;
    case ExpectedCas::Any:
        break;
    }

    if ((~expected_datatype_mask) & header.getDatatype()) {
        cookie.setErrorContext("Request datatype invalid");
        return false;
    }

    return true;
}

/******************************************************************************
 *                         Package validators                                 *
 *****************************************************************************/

/**
 * Verify that the cookie meets the common DCP restrictions:
 *
 * a) The connected engine supports DCP
 * b) The connection cannot be set into the unordered execution mode.
 *
 * In the future it should be extended to verify that the various DCP
 * commands is only sent on a connection which is set up as a DCP
 * connection (except the initial OPEN etc)
 *
 * @param cookie The command cookie
 */
static Status verify_common_dcp_restrictions(Cookie& cookie) {
    auto* dcp = cookie.getConnection().getBucket().getDcpIface();
    if (!dcp) {
        cookie.setErrorContext("Attached bucket does not support DCP");
        return Status::NotSupported;
    }

    const auto& connection = cookie.getConnection();
    if (connection.allowUnorderedExecution()) {
        LOG_WARNING(
                "DCP on a connection with unordered execution is currently "
                "not supported: {}",
                get_peer_description(cookie));
        cookie.setErrorContext(
                "DCP on connections with unordered execution is not supported");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status dcp_open_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_dcp_open*>(
            cookie.getPacketAsVoidPtr());

    if (!verify_header(cookie,
                       8,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    const auto mask = DCP_OPEN_PRODUCER | DCP_OPEN_NOTIFIER |
                      DCP_OPEN_INCLUDE_XATTRS | DCP_OPEN_NO_VALUE |
                      DCP_OPEN_INCLUDE_DELETE_TIMES;

    const auto flags = ntohl(req->message.body.flags);

    if (flags & ~mask) {
        LOG_INFO(
                "Client trying to open dcp stream with unknown flags ({:x}) {}",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext("Request contains invalid flags");
        return Status::Einval;
    }

    if ((flags & DCP_OPEN_NOTIFIER) && (flags & ~DCP_OPEN_NOTIFIER)) {
        LOG_INFO(
                "Invalid flags combination ({:x}) specified for a DCP "
                "consumer {}",
                flags,
                get_peer_description(cookie));
        cookie.setErrorContext("Request contains invalid flags combination");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_add_stream_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_dcp_add_stream*>(
            cookie.getPacketAsVoidPtr());

    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    const auto flags = ntohl(req->message.body.flags);
    const auto mask = DCP_ADD_STREAM_FLAG_TAKEOVER |
                      DCP_ADD_STREAM_FLAG_DISKONLY |
                      DCP_ADD_STREAM_FLAG_LATEST |
                      DCP_ADD_STREAM_ACTIVE_VB_ONLY;

    if (flags & ~mask) {
        if (flags & DCP_ADD_STREAM_FLAG_NO_VALUE) {
            // MB-22525 The NO_VALUE flag should be passed to DCP_OPEN
            LOG_INFO("Client trying to add stream with NO VALUE {}",
                     get_peer_description(cookie));
            cookie.setErrorContext(
                    "DCP_ADD_STREAM_FLAG_NO_VALUE{8} flag is no longer used");
        } else {
            LOG_INFO("Client trying to add stream with unknown flags ({:x}) {}",
                     flags,
                     get_peer_description(cookie));
            cookie.setErrorContext("Request contains invalid flags");
        }
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_close_stream_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_get_failover_log_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_req_validator(Cookie& cookie) {
    constexpr uint8_t expected_extlen =
            5 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

    auto vlen = cookie.getConnection().isCollectionsSupported()
                        ? ExpectedValueLen::Any
                        : ExpectedValueLen::Zero;

    if (!verify_header(cookie,
                       expected_extlen,
                       ExpectedKeyLen::Zero,
                       vlen,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_stream_end_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_snapshot_marker_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       20,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_system_event_validator(Cookie& cookie) {
    // keylen + bodylen > ??
    auto req = static_cast<protocol_binary_request_dcp_system_event*>(
            cookie.getPacketAsVoidPtr());

    if (!verify_header(
                cookie,
                protocol_binary_request_dcp_system_event::getExtrasLength(),
                ExpectedKeyLen::Any,
                ExpectedValueLen::Any)) {
        return Status::Einval;
    }

    if (!mcbp::systemevent::validate_id(ntohl(req->message.body.event))) {
        cookie.setErrorContext("Invalid system event id");
        return Status::Einval;
    }

    if (!mcbp::systemevent::validate_version(req->message.body.version)) {
        cookie.setErrorContext("Invalid system event version");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static bool is_valid_xattr_blob(const protocol_binary_request_header& header) {
    const uint32_t extlen{header.request.extlen};
    const uint32_t keylen{ntohs(header.request.keylen)};
    const uint32_t bodylen{ntohl(header.request.bodylen)};

    auto* ptr = reinterpret_cast<const char*>(header.bytes);
    ptr += sizeof(header.bytes) + extlen + keylen;

    cb::compression::Buffer buffer;
    cb::const_char_buffer xattr{ptr, bodylen - keylen - extlen};
    if (mcbp::datatype::is_snappy(header.request.datatype)) {
        // Inflate the xattr data and validate that.
        if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy, xattr, buffer)) {
            return false;
        }
        xattr = buffer;
    }

    return cb::xattr::validate(xattr);
}

static Status dcp_mutation_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_dcp_mutation*>(
            cookie.getPacketAsVoidPtr());
    const auto datatype = req->message.header.request.datatype;

    if (!verify_header(cookie,
                       protocol_binary_request_dcp_mutation::getExtrasLength(),
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    if (!may_accept_xattr(cookie)) {
        cookie.setErrorContext("Connection not Xattr enabled");
        return Status::Einval;
    }

    if (mcbp::datatype::is_xattr(datatype) &&
        !is_valid_xattr_blob(req->message.header)) {
        cookie.setErrorContext("Xattr blob not valid");
        return Status::XattrEinval;
    }
    return verify_common_dcp_restrictions(cookie);
}

/// @return true if the datatype is valid for a deletion
static bool valid_dcp_delete_datatype(protocol_binary_datatype_t datatype) {
    // MB-29040: Allowing xattr + JSON. A bug in the producer means
    // it may send XATTR|JSON (with snappy possible). These are now allowed
    // so rebalance won't be failed and the consumer will sanitise the faulty
    // documents.
    // MB-31141: Allowing RAW+Snappy. A bug in delWithMeta has allowed us to
    // create deletes with a non-zero value tagged as RAW, which when snappy
    // is enabled gets DCP shipped as RAW+Snappy.
    std::array<const protocol_binary_datatype_t, 6> valid = {
            {PROTOCOL_BINARY_RAW_BYTES,
             PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_SNAPPY,
             PROTOCOL_BINARY_DATATYPE_XATTR,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON,
             PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_SNAPPY |
                     PROTOCOL_BINARY_DATATYPE_JSON}};
    for (auto d : valid) {
        if (datatype == d) {
            return true;
        }
    }
    return false;
}

static Status dcp_deletion_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_dcp_deletion*>(
            cookie.getPacketAsVoidPtr());

    const uint8_t expectedExtlen =
            may_accept_dcp_deleteV2(cookie)
                    ? protocol_binary_request_dcp_deletion_v2::extlen
                    : protocol_binary_request_dcp_deletion::extlen;

    if (!verify_header(cookie,
                       expectedExtlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }
    if (!valid_dcp_delete_datatype(req->message.header.request.datatype)) {
        cookie.setErrorContext("Request datatype invalid");
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_expiration_validator(Cookie& cookie) {
    if (!verify_header(
                cookie,
                gsl::narrow<uint8_t>(protocol_binary_request_dcp_expiration::
                                             getExtrasLength()),
                ExpectedKeyLen::NonZero,
                ExpectedValueLen::Zero,
                ExpectedCas::Any,
                PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_set_vbucket_state_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_dcp_set_vbucket_state*>(
            cookie.getPacketAsVoidPtr());

    if (!verify_header(cookie,
                       1,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (req->message.body.state < 1 || req->message.body.state > 4) {
        cookie.setErrorContext("Request body state invalid");
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_noop_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_buffer_acknowledgement_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status dcp_control_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return verify_common_dcp_restrictions(cookie);
}

static Status update_user_permissions_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status configuration_refresh_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status auth_provider_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status verbosity_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status hello_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_no_extras*>(
            cookie.getPacketAsVoidPtr());
    uint32_t len = ntohl(req->message.header.request.bodylen);
    len -= ntohs(req->message.header.request.keylen);

    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if ((len % 2) != 0) {
        cookie.setErrorContext("Request value must be of even length");
        return Status::Einval;
    }

    return Status::Success;
}

static Status version_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status quit_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status sasl_list_mech_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status sasl_auth_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status noop_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status flush_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_no_extras*>(
            cookie.getPacketAsVoidPtr());
    uint8_t extlen = req->message.header.request.extlen;

    if (extlen != 0 && extlen != 4) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }
    // We've already checked extlen so pass actual extlen as expected extlen
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (extlen == 4) {
        auto* req = reinterpret_cast<protocol_binary_request_flush*>(
                cookie.getPacketAsVoidPtr());
        if (req->message.body.expiration != 0) {
            cookie.setErrorContext("Delayed flush no longer supported");
            return Status::NotSupported;
        }
    }

    return Status::Success;
}

static Status add_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    if (!verify_header(cookie,
                       8,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_replace_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must have extras and key, may have value */
    if (!verify_header(cookie,
                       8,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status append_prepend_validator(Cookie& cookie) {
    constexpr uint8_t expected_datatype_mask = PROTOCOL_BINARY_RAW_BYTES |
                                               PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_SNAPPY;
    /* Must not have extras, must have key, may have value */
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       expected_datatype_mask)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status gat_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status delete_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status stat_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status arithmetic_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       20,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_cmd_timer_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       1,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status set_ctrl_token_validator(Cookie& cookie) {
    constexpr uint8_t expected_extlen = sizeof(uint64_t);

    if (!verify_header(cookie,
                       expected_extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto req = static_cast<protocol_binary_request_set_ctrl_token*>(
            cookie.getPacketAsVoidPtr());

    if (req->message.body.new_cas == 0) {
        cookie.setErrorContext("New CAS must be set");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_ctrl_token_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status ioctl_get_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > IOCTL_KEY_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status ioctl_set_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    auto req = static_cast<protocol_binary_request_ioctl_set*>(
            cookie.getPacketAsVoidPtr());
    uint16_t klen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - klen;

    if (klen > IOCTL_KEY_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }
    if (vallen > IOCTL_VAL_LENGTH) {
        cookie.setErrorContext("Request value length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status audit_put_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       4,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status audit_config_reload_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status config_reload_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status config_validate_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    auto& header = cookie.getHeader();
    const auto bodylen = header.getBodylen();

    if (bodylen > CONFIG_VALIDATE_MAX_LENGTH) {
        cookie.setErrorContext("Request value length exceeds maximum");
        return Status::Einval;
    }
    return Status::Success;
}

static Status observe_seqno_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getBodylen() != 8) {
        cookie.setErrorContext("Request value must be of length 8");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_adjusted_time_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status set_drift_counter_state_validator(Cookie& cookie) {
    constexpr uint8_t expected_extlen = sizeof(uint8_t) + sizeof(int64_t);

    if (!verify_header(cookie,
                       expected_extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static Status create_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > MAX_BUCKET_NAME_LENGTH) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status list_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status delete_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    return Status::Success;
}

static Status select_bucket_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Any,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getKeylen() > 1023) {
        cookie.setErrorContext("Request key length exceeds maximum");
        return Status::Einval;
    }

    return Status::Success;
}

static Status get_all_vb_seqnos_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_get_all_vb_seqnos*>(
            cookie.getPacketAsVoidPtr());
    uint8_t extlen = req->message.header.request.extlen;

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in verify_header
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (extlen != 0) {
        // extlen is optional, and if non-zero it contains the vbucket
        // state to report
        if (extlen != sizeof(vbucket_state_t)) {
            cookie.setErrorContext("Request extras must be of length 0 or " +
                                   std::to_string(sizeof(vbucket_state_t)));
            return Status::Einval;
        }
        vbucket_state_t state;
        memcpy(&state, &req->message.body.state, sizeof(vbucket_state_t));
        state = static_cast<vbucket_state_t>(ntohl(state));
        if (!is_valid_vbucket_state_t(state)) {
            cookie.setErrorContext("Request vbucket state invalid");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status shutdown_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Set,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_meta_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_no_extras*>(
            cookie.getPacketAsVoidPtr());
    uint32_t extlen = req->message.header.request.extlen;

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in verify_header
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (extlen > 1) {
        cookie.setErrorContext("Request extras must be of length 0 or 1");
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    if (extlen == 1) {
        const uint8_t* extdata = req->bytes + sizeof(req->bytes);
        if (*extdata > 2) {
            // 1 == return conflict resolution mode
            // 2 == return datatype
            cookie.setErrorContext("Request extras invalid");
            return Status::Einval;
        }
    }

    return Status::Success;
}

static Status mutate_with_meta_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_get_meta*>(
            cookie.getPacketAsVoidPtr());

    const uint32_t extlen = req->message.header.request.extlen;
    const auto datatype = req->message.header.request.datatype;

    // We check extlen below so pass actual extlen as expected_extlen to bypass
    // the check in verify_header
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Any)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    if (!may_accept_xattr(cookie)) {
        cookie.setErrorContext("Connection not Xattr enabled");
        return Status::Einval;
    }

    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    // extlen, the size dicates what is encoded.
    switch (extlen) {
    case 24: // no nmeta and no options
    case 26: // nmeta
    case 28: // options (4-byte field)
    case 30: // options and nmeta (options followed by nmeta)
        break;
    default:
        cookie.setErrorContext("Request extras invalid");
        return Status::Einval;
    }

    if (mcbp::datatype::is_xattr(datatype) &&
        !is_valid_xattr_blob(req->message.header)) {
        cookie.setErrorContext("Xattr blob invalid");
        return Status::XattrEinval;
    }

    return Status::Success;
}

static Status get_errmap_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Any,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    const auto& hdr = *static_cast<const protocol_binary_request_header*>(
            cookie.getPacketAsVoidPtr());

    if (hdr.request.vbucket != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }
    if (hdr.request.getBodylen() != 2) {
        cookie.setErrorContext("Request value must be of length 2");
        return Status::Einval;
    }
    return Status::Success;
}

static Status get_locked_validator(Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_no_extras*>(
            cookie.getPacketAsVoidPtr());
    uint32_t extlen = req->message.header.request.extlen;

    // We check extlen below so pass actual extlen as expected extlen to bypass
    // the check in verify header
    if (!verify_header(cookie,
                       extlen,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    if (extlen != 0 && extlen != 4) {
        cookie.setErrorContext("Request extras must be of length 0 or 4");
        return Status::Einval;
    }

    return Status::Success;
}

static Status unlock_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Set,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }

    return Status::Success;
}

static Status evict_key_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::NonZero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (!is_document_key_valid(cookie)) {
        cookie.setErrorContext("Request key invalid");
        return Status::Einval;
    }
    return Status::Success;
}

static Status collections_set_manifest_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::NonZero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }
    if (cookie.getHeader().getRequest().getVBucket() != Vbid(0)) {
        cookie.setErrorContext("Request vbucket id must be 0");
        return Status::Einval;
    }

    // We could do these tests before checking the packet, but
    // it feels cleaner to validate the packet first.
    auto* engine = cookie.getConnection().getBucket().getEngine();
    if (engine == nullptr || engine->collections.set_manifest == nullptr) {
        cookie.setErrorContext("Attached bucket does not support collections");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status collections_get_manifest_validator(Cookie& cookie) {
    if (!verify_header(cookie,
                       0,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::Any,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    // We could do these tests before checking the packet, but
    // it feels cleaner to validate the packet first.
    auto* engine = cookie.getConnection().getBucket().getEngine();
    if (engine == nullptr || engine->collections.get_manifest == nullptr) {
        cookie.setErrorContext("Attached bucket does not support collections");
        return Status::NotSupported;
    }

    return Status::Success;
}

static Status adjust_timeofday_validator(Cookie& cookie) {
    constexpr uint8_t expected_extlen = sizeof(uint64_t) + sizeof(uint8_t);

    if (!verify_header(cookie,
                       expected_extlen,
                       ExpectedKeyLen::Zero,
                       ExpectedValueLen::Zero,
                       ExpectedCas::NotSet,
                       PROTOCOL_BINARY_RAW_BYTES)) {
        return Status::Einval;
    }

    // The method should only be available for unit tests
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        cookie.setErrorContext("Only available for unit tests");
        return Status::NotSupported;
    }

    return Status::Success;
}

Status McbpValidator::validate(ClientOpcode command, Cookie& cookie) {
    const auto idx = std::underlying_type<ClientOpcode>::type(command);
    if (validators[idx]) {
        return validators[idx](cookie);
    }
    return Status::Success;
}

void McbpValidator::setup(ClientOpcode command, Status (*f)(Cookie&)) {
    validators[std::underlying_type<ClientOpcode>::type(command)] = f;
}

McbpValidator::McbpValidator() {
    setup(cb::mcbp::ClientOpcode::DcpOpen, dcp_open_validator);
    setup(cb::mcbp::ClientOpcode::DcpAddStream, dcp_add_stream_validator);
    setup(cb::mcbp::ClientOpcode::DcpCloseStream, dcp_close_stream_validator);
    setup(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
          dcp_snapshot_marker_validator);
    setup(cb::mcbp::ClientOpcode::DcpDeletion, dcp_deletion_validator);
    setup(cb::mcbp::ClientOpcode::DcpExpiration, dcp_expiration_validator);
    setup(cb::mcbp::ClientOpcode::DcpGetFailoverLog,
          dcp_get_failover_log_validator);
    setup(cb::mcbp::ClientOpcode::DcpMutation, dcp_mutation_validator);
    setup(cb::mcbp::ClientOpcode::DcpSetVbucketState,
          dcp_set_vbucket_state_validator);
    setup(cb::mcbp::ClientOpcode::DcpNoop, dcp_noop_validator);
    setup(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
          dcp_buffer_acknowledgement_validator);
    setup(cb::mcbp::ClientOpcode::DcpControl, dcp_control_validator);
    setup(cb::mcbp::ClientOpcode::DcpStreamEnd, dcp_stream_end_validator);
    setup(cb::mcbp::ClientOpcode::DcpStreamReq, dcp_stream_req_validator);
    setup(cb::mcbp::ClientOpcode::DcpSystemEvent, dcp_system_event_validator);
    setup(cb::mcbp::ClientOpcode::IsaslRefresh,
          configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::SslCertsRefresh,
          configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::Verbosity, verbosity_validator);
    setup(cb::mcbp::ClientOpcode::Hello, hello_validator);
    setup(cb::mcbp::ClientOpcode::Version, version_validator);
    setup(cb::mcbp::ClientOpcode::Quit, quit_validator);
    setup(cb::mcbp::ClientOpcode::Quitq, quit_validator);
    setup(cb::mcbp::ClientOpcode::SaslListMechs, sasl_list_mech_validator);
    setup(cb::mcbp::ClientOpcode::SaslAuth, sasl_auth_validator);
    setup(cb::mcbp::ClientOpcode::SaslStep, sasl_auth_validator);
    setup(cb::mcbp::ClientOpcode::Noop, noop_validator);
    setup(cb::mcbp::ClientOpcode::Flush, flush_validator);
    setup(cb::mcbp::ClientOpcode::Flushq, flush_validator);
    setup(cb::mcbp::ClientOpcode::Get, get_validator);
    setup(cb::mcbp::ClientOpcode::Getq, get_validator);
    setup(cb::mcbp::ClientOpcode::Getk, get_validator);
    setup(cb::mcbp::ClientOpcode::Getkq, get_validator);
    setup(cb::mcbp::ClientOpcode::Gat, gat_validator);
    setup(cb::mcbp::ClientOpcode::Gatq, gat_validator);
    setup(cb::mcbp::ClientOpcode::Touch, gat_validator);
    setup(cb::mcbp::ClientOpcode::Delete, delete_validator);
    setup(cb::mcbp::ClientOpcode::Deleteq, delete_validator);
    setup(cb::mcbp::ClientOpcode::Stat, stat_validator);
    setup(cb::mcbp::ClientOpcode::Increment, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Incrementq, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Decrement, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::Decrementq, arithmetic_validator);
    setup(cb::mcbp::ClientOpcode::GetCmdTimer, get_cmd_timer_validator);
    setup(cb::mcbp::ClientOpcode::SetCtrlToken, set_ctrl_token_validator);
    setup(cb::mcbp::ClientOpcode::GetCtrlToken, get_ctrl_token_validator);
    setup(cb::mcbp::ClientOpcode::IoctlGet, ioctl_get_validator);
    setup(cb::mcbp::ClientOpcode::IoctlSet, ioctl_set_validator);
    setup(cb::mcbp::ClientOpcode::AuditPut, audit_put_validator);
    setup(cb::mcbp::ClientOpcode::AuditConfigReload,
          audit_config_reload_validator);
    setup(cb::mcbp::ClientOpcode::ConfigReload, config_reload_validator);
    setup(cb::mcbp::ClientOpcode::ConfigValidate, config_validate_validator);
    setup(cb::mcbp::ClientOpcode::Shutdown, shutdown_validator);
    setup(cb::mcbp::ClientOpcode::ObserveSeqno, observe_seqno_validator);
    setup(cb::mcbp::ClientOpcode::GetAdjustedTime, get_adjusted_time_validator);
    setup(cb::mcbp::ClientOpcode::SetDriftCounterState,
          set_drift_counter_state_validator);

    setup(cb::mcbp::ClientOpcode::SubdocGet, subdoc_get_validator);
    setup(cb::mcbp::ClientOpcode::SubdocExists, subdoc_exists_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDictAdd, subdoc_dict_add_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDictUpsert,
          subdoc_dict_upsert_validator);
    setup(cb::mcbp::ClientOpcode::SubdocDelete, subdoc_delete_validator);
    setup(cb::mcbp::ClientOpcode::SubdocReplace, subdoc_replace_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
          subdoc_array_push_last_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
          subdoc_array_push_first_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayInsert,
          subdoc_array_insert_validator);
    setup(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
          subdoc_array_add_unique_validator);
    setup(cb::mcbp::ClientOpcode::SubdocCounter, subdoc_counter_validator);
    setup(cb::mcbp::ClientOpcode::SubdocMultiLookup,
          subdoc_multi_lookup_validator);
    setup(cb::mcbp::ClientOpcode::SubdocMultiMutation,
          subdoc_multi_mutation_validator);
    setup(cb::mcbp::ClientOpcode::SubdocGetCount, subdoc_get_count_validator);

    setup(cb::mcbp::ClientOpcode::Setq, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Set, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Addq, add_validator);
    setup(cb::mcbp::ClientOpcode::Add, add_validator);
    setup(cb::mcbp::ClientOpcode::Replaceq, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Replace, set_replace_validator);
    setup(cb::mcbp::ClientOpcode::Appendq, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Append, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Prependq, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::Prepend, append_prepend_validator);
    setup(cb::mcbp::ClientOpcode::CreateBucket, create_bucket_validator);
    setup(cb::mcbp::ClientOpcode::ListBuckets, list_bucket_validator);
    setup(cb::mcbp::ClientOpcode::DeleteBucket, delete_bucket_validator);
    setup(cb::mcbp::ClientOpcode::SelectBucket, select_bucket_validator);
    setup(cb::mcbp::ClientOpcode::GetAllVbSeqnos, get_all_vb_seqnos_validator);

    setup(cb::mcbp::ClientOpcode::EvictKey, evict_key_validator);

    setup(cb::mcbp::ClientOpcode::GetMeta, get_meta_validator);
    setup(cb::mcbp::ClientOpcode::GetqMeta, get_meta_validator);
    setup(cb::mcbp::ClientOpcode::SetWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::SetqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::AddWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::AddqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::DelWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::DelqWithMeta, mutate_with_meta_validator);
    setup(cb::mcbp::ClientOpcode::GetErrorMap, get_errmap_validator);
    setup(cb::mcbp::ClientOpcode::GetLocked, get_locked_validator);
    setup(cb::mcbp::ClientOpcode::UnlockKey, unlock_validator);
    setup(cb::mcbp::ClientOpcode::UpdateExternalUserPermissions,
          update_user_permissions_validator);
    setup(cb::mcbp::ClientOpcode::RbacRefresh, configuration_refresh_validator);
    setup(cb::mcbp::ClientOpcode::AuthProvider, auth_provider_validator);
    setup(cb::mcbp::ClientOpcode::GetFailoverLog,
          dcp_get_failover_log_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsSetManifest,
          collections_set_manifest_validator);
    setup(cb::mcbp::ClientOpcode::CollectionsGetManifest,
          collections_get_manifest_validator);
    setup(cb::mcbp::ClientOpcode::AdjustTimeofday, adjust_timeofday_validator);
}

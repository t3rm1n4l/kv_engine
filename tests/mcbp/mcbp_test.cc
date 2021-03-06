/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "mcbp_test.h"

#include <daemon/cookie.h>
#include <daemon/settings.h>
#include <event2/event.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <gsl/gsl>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp {
namespace test {

ValidatorTest::ValidatorTest(bool collectionsEnabled)
    : request(*reinterpret_cast<protocol_binary_request_no_extras*>(blob)),
      collectionsEnabled(collectionsEnabled) {
}

void ValidatorTest::SetUp() {
    settings.setXattrEnabled(true);
    connection.setCollectionsSupported(collectionsEnabled);
    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
}

/**
 * Mock the cookie class and override the getPacket method so that we
 * may use the buffer directly instead of having to insert it into the read/
 * write buffers of the underlying connection
 */
class MockCookie : public Cookie {
public:
    MockCookie(Connection& connection, cb::const_byte_buffer buffer)
        : Cookie(connection) {
        setPacket(PacketContent::Full, buffer);
    }
};

cb::mcbp::Status ValidatorTest::validate(cb::mcbp::ClientOpcode opcode,
                                         void* packet) {
    // Mockup a McbpConnection and Cookie for the validator chain
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    return validatorChains.validate(opcode, cookie);
}

std::string ValidatorTest::validate_error_context(cb::mcbp::ClientOpcode opcode,
                                                  void* packet) {
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    validatorChains.validate(opcode, cookie);
    return cookie.getErrorContext();
}

enum class GetOpcodes : uint8_t {
    Get = PROTOCOL_BINARY_CMD_GET,
    GetQ = PROTOCOL_BINARY_CMD_GETQ,
    GetK = PROTOCOL_BINARY_CMD_GETK,
    GetKQ = PROTOCOL_BINARY_CMD_GETKQ,
    GetMeta = PROTOCOL_BINARY_CMD_GET_META,
    GetQMeta = PROTOCOL_BINARY_CMD_GETQ_META
};

std::string to_string(const GetOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case GetOpcodes::Get:
        return "Get";
    case GetOpcodes::GetQ:
        return "GetQ";
    case GetOpcodes::GetK:
        return "GetK";
    case GetOpcodes::GetKQ:
        return "GetKQ";
    case GetOpcodes::GetMeta:
        return "GetMeta";
    case GetOpcodes::GetQMeta:
        return "GetQMeta";
    }
    throw std::invalid_argument("to_string(): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const GetOpcodes& o) {
    os << to_string(o);
    return os;
}

// Test the validators for GET, GETQ, GETK, GETKQ, GET_META and GETQ_META
class GetValidatorTest
    : public ::testing::WithParamInterface<std::tuple<GetOpcodes, bool>>,
      public ValidatorTest {
public:
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    GetValidatorTest()
        : ValidatorTest(std::get<1>(GetParam())),
          bodylen(request.message.header.request.bodylen) {
        // empty
    }

    GetOpcodes getGetOpcode() const {
        return std::get<0>(GetParam());
    }

    bool isCollectionsEnabled() const {
        return std::get<1>(GetParam());
    }

protected:
    cb::mcbp::Status validateExtendedExtlen(uint8_t version) {
        bodylen = htonl(ntohl(bodylen) + 1);
        request.message.header.request.extlen = 1;
        blob[sizeof(protocol_binary_request_get)] = version;
        return validate();
    }

    cb::mcbp::Status validate() {
        auto opcode = cb::mcbp::ClientOpcode(std::get<0>(GetParam()));
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    uint32_t& bodylen;
};

TEST_P(GetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, ExtendedExtlenV1) {
    switch (getGetOpcode()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(cb::mcbp::Status::Success, validateExtendedExtlen(1));
        break;
    }
}

TEST_P(GetValidatorTest, ExtendedExtlenV2) {
    switch (getGetOpcode()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(cb::mcbp::Status::Success, validateExtendedExtlen(2));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtendedExtlenVersion) {
    switch (getGetOpcode()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(cb::mcbp::Status::Einval, validateExtendedExtlen(3));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtlen) {
    bodylen = htonl(ntohl(bodylen) + 21);
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen =
            isCollectionsEnabled() ? htons(1) : 0;
    bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    std::fill(blob + sizeof(request.bytes),
              blob + sizeof(request.bytes) + 10,
              0x81ull);
    request.message.header.request.keylen = htons(10);
    bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}


// @todo add test case for the extra legal modes for the
// get meta case

INSTANTIATE_TEST_CASE_P(
        GetOpcodes,
        GetValidatorTest,
        ::testing::Combine(::testing::Values(GetOpcodes::Get,
                                             GetOpcodes::GetQ,
                                             GetOpcodes::GetK,
                                             GetOpcodes::GetKQ,
                                             GetOpcodes::GetMeta,
                                             GetOpcodes::GetQMeta),
                           ::testing::Bool()), );

// Test ADD & ADDQ
class AddValidatorTest : public ::testing::WithParamInterface<bool>,
                         public ValidatorTest {
public:
    AddValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(AddValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    auto fill = blob + request.message.header.request.extlen;
    std::fill(fill + sizeof(request.bytes),
              fill + sizeof(request.bytes) + 10,
              0x80ull);
    request.message.header.request.keylen = htons(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

// Test SET, SETQ, REPLACE, REPLACEQ
class SetReplaceValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    SetReplaceValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(SetReplaceValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, (no stop-byte) illegal.
    auto key = blob + sizeof(request.bytes) +
               request.message.header.request.extlen;
    std::fill(key, key + 10, 0x80ull);
    request.message.header.request.keylen = htons(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

// Test Append[q] and Prepend[q]
class AppendPrependValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    AppendPrependValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(AppendPrependValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen =
            isCollectionsEnabled() ? htons(1) : 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

// Test DELETE & DELETEQ
class DeleteValidatorTest : public ::testing::WithParamInterface<bool>,
                            public ValidatorTest {
public:
    DeleteValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(DeleteValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen =
            isCollectionsEnabled() ? htons(1) : 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

// Test INCREMENT[q] and DECREMENT[q]
class IncrementDecrementValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    IncrementDecrementValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(30);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(IncrementDecrementValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.message.header.request.keylen =
            isCollectionsEnabled() ? htons(1) : 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(40);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

// Test QUIT & QUITQ
class QuitValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    QuitValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(QuitValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

// Test FLUSH & FLUSHQ
class FlushValidatorTest : public ::testing::WithParamInterface<bool>,
                           public ValidatorTest {
public:
    FlushValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(FlushValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, CorrectMessageWithTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, CorrectMessageWithUnsupportedTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    *reinterpret_cast<uint32_t*>(request.bytes + sizeof(request.bytes)) = 1;
    EXPECT_EQ(cb::mcbp::Status::NotSupported,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::NotSupported,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

TEST_P(FlushValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flush));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Flushq));
}

// test Noop
class NoopValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    NoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Noop,
                                       static_cast<void*>(&request));
    }
};

TEST_P(NoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(NoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test version
class VersionValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    VersionValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Version,
                                       static_cast<void*>(&request));
    }
};

TEST_P(VersionValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(VersionValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test stat
class StatValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    StatValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Stat,
                                       static_cast<void*>(&request));
    }
};

TEST_P(StatValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(StatValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(StatValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test verbosity
class VerbosityValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    VerbosityValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Verbosity,
                                       static_cast<void*>(&request));
    }
};

TEST_P(VerbosityValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(VerbosityValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test HELLO
class HelloValidatorTest : public ::testing::WithParamInterface<bool>,
                           public ValidatorTest {
public:
    HelloValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Hello,
                                       static_cast<void*>(&request));
    }
};

TEST_P(HelloValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, MultipleFeatures) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    request.message.header.request.bodylen = htonl(6);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test SASL_LIST_MECHS
class SaslListMechValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    SaslListMechValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SaslListMechs,
                                       static_cast<void*>(&request));
    }
};

TEST_P(SaslListMechValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test SASL_AUTH
class SaslAuthValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    SaslAuthValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(SaslAuthValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, WithChallenge) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

class GetErrmapValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    GetErrmapValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetErrorMap,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetErrmapValidatorTest, CorrectMessage) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetErrmapValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetErrmapValidatorTest, MissingBody) {
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test IOCTL_GET
class IoctlGetValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    IoctlGetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;

    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::IoctlGet,
                                       static_cast<void*>(&request));
    }
};

TEST_P(IoctlGetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test IOCTL_SET
class IoctlSetValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    IoctlSetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;
    const int IOCTL_VAL_LENGTH = 128;

    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::IoctlSet,
                                       static_cast<void*>(&request));
    }
};

TEST_P(IoctlSetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, ValidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 10);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

// test AUDIT_PUT
class AuditPutValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    AuditPutValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::AuditPut,
                                       static_cast<void*>(&request));
    }
};

TEST_P(AuditPutValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(AuditPutValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(15);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test audit_config_reload
class AuditConfigReloadValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    AuditConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::AuditConfigReload,
                static_cast<void*>(&request));
    }
};

TEST_P(AuditConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test shutdown
class ShutdownValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    ShutdownValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.cas = 1;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Shutdown,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ShutdownValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ShutdownValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpOpenValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    DcpOpenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpOpen,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_open &request = *reinterpret_cast<protocol_binary_request_dcp_open*>(blob);
};

TEST_P(DcpOpenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 9;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, Value) {
    request.message.header.request.bodylen = htonl(10 + 20);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpAddStreamValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpAddStreamValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpAddStream,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpCloseStreamValidatorTest : public ::testing::WithParamInterface<bool>,
                                    public ValidatorTest {
public:
    DcpCloseStreamValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpCloseStream,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpGetFailoverLogValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpGetFailoverLogValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpStreamReqValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpStreamReqValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 48;
        request.message.header.request.bodylen = htonl(48);
    }
    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamReq,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(54);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, MessageValue) {
    request.message.header.request.bodylen = htonl(48 + 20);
    // Only valid when collections enabled
    if (isCollectionsEnabled()) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());

    } else {
        EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    }
}

class DcpStreamEndValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpStreamEndValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamEnd,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpSnapshotMarkerValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpSnapshotMarkerValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 32;
    request.message.header.request.bodylen = htonl(52);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

/**
 * Test class for DcpMutation validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a mutation)
 */
class DcpMutationValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
public:
public:
    DcpMutationValidatorTest()
        : ValidatorTest(GetParam()),
          request(0 /*opaque*/,
                  Vbid(0),
                  0 /*cas*/,
                  GetParam() ? 2 : 1 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*flags*/,
                  0 /*expiration*/,
                  0 /*lockTime*/,
                  0 /*nmeta*/,
                  0 /*nru*/) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpMutation,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_mutation request;
};


TEST_P(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(22);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlenCollections) {
    request.message.header.request.extlen =
            protocol_binary_request_dcp_mutation::getExtrasLength() + 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    request.message.header.request.bodylen = htonl(31);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// A key which has no leb128 stop-byte
TEST_P(DcpMutationValidatorTest, InvalidKey1) {
    if (isCollectionsEnabled()) {
        std::fill(blob + sizeof(request.bytes),
                  blob + sizeof(request.bytes) + 10,
                  0x81ull);
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen =
                htonl(request.message.header.request.extlen + 10);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    }
}

// A key which has a stop-byte, but no data after that
TEST_P(DcpMutationValidatorTest, InvalidKey2) {
    if (isCollectionsEnabled()) {
        std::fill(blob + sizeof(request.bytes),
                  blob + sizeof(request.bytes) + 9,
                  0x81ull);
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen =
                htonl(request.message.header.request.extlen + 10);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    }
}

/**
 * Test class for DcpDeletion validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a deletion)
 */
class DcpDeletionValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
public:
public:
    DcpDeletionValidatorTest()
        : ValidatorTest(GetParam()),
          request(GetParam() ? makeV2() : makeV1()),
          header(request->getHeader()) {
        header.request.opcode = (uint8_t)cb::mcbp::ClientOpcode::DcpDeletion;
        if (GetParam()) {
            header.request.keylen = htons(5); // min-collection key
            header.request.bodylen = htonl(header.request.extlen + 5);
        }
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        std::copy(request->getBytes(),
                  request->getBytes() + request->getSizeofBytes(),
                  blob);
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpDeletion,
                                       static_cast<void*>(blob));
    }

    class Request {
    public:
        virtual ~Request() = default;
        virtual protocol_binary_request_header& getHeader() = 0;

        virtual uint8_t* getBytes() = 0;

        virtual size_t getSizeofBytes() = 0;
    };

    class RequestV1 : public Request {
    public:
        RequestV1()
            : request(0 /*opaque*/,
                      Vbid(0),
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0 /*nmeta*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion request;
    };

    class RequestV2 : public Request {
    public:
        RequestV2()
            : request(0 /*opaque*/,
                      Vbid(0),
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0, /*deleteTime*/
                      0 /*collectionLen*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion_v2 request;
    };

    std::unique_ptr<Request> makeV1() {
        return std::make_unique<RequestV1>();
    }

    std::unique_ptr<Request> makeV2() {
        return std::make_unique<RequestV2>();
    }

    std::unique_ptr<Request> request;
    protocol_binary_request_header& header;
};


TEST_P(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidMagic) {
    header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, ValidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 4> datatypes = {
            {uint8_t(Datatype::Raw),
             uint8_t(Datatype::Raw) | uint8_t(Datatype::Snappy),
             uint8_t(Datatype::Xattr),
             uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy)}};

    for (auto valid : datatypes) {
        header.request.datatype = valid;
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate())
                << "Testing valid datatype:" << int(valid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 2> datatypes = {
            {uint8_t(Datatype::JSON),
             uint8_t(Datatype::Snappy) | uint8_t(Datatype::JSON)}};

    for (auto invalid : datatypes) {
        header.request.datatype = invalid;
        EXPECT_EQ(cb::mcbp::Status::Einval, validate())
                << "Testing invalid datatype:" << int(invalid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlen) {
    header.request.extlen = 5;
    header.request.bodylen = htonl(7);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlenCollections) {
    // Flip extlen, so when not collections, set the length collections uses
    header.request.extlen =
            isCollectionsEnabled()
                    ? protocol_binary_request_dcp_deletion::extlen
                    : protocol_binary_request_dcp_deletion_v2::extlen;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidKeylen) {
    header.request.keylen = GetParam() ? htons(1) : 0;
    header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, WithValue) {
    header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

/**
 * Test class for DcpExpiration validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of an expiration)
 */
class DcpExpirationValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
public:
    DcpExpirationValidatorTest()
        : ValidatorTest(GetParam()),
          request(0 /*opaque*/,
                  Vbid(0),
                  0 /*cas*/,
                  GetParam() ? 5 : 1 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*nmeta*/) {
        request.message.header.request.opcode =
                (uint8_t)cb::mcbp::ClientOpcode::DcpExpiration;
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setCollectionsSupported(GetParam());
    }

protected:
    cb::mcbp::Status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpExpiration,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_expiration request;
};

TEST_P(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(7);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = GetParam() ? htons(1) : 0;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpSetVbucketStateValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpSetVbucketStateValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.state = 1;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSetVbucketState,
                static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_set_vbucket_state &request =
       *reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(blob);
};

TEST_P(DcpSetVbucketStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, LegalValues) {
    for (int ii = 1; ii < 5; ++ii) {
        request.message.body.state = ii;
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
    }
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, IllegalValues) {
    request.message.body.state = 5;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    request.message.body.state = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpNoopValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    DcpNoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpNoop,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpBufferAckValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpBufferAckValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpControlValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    DcpControlValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(4);
        request.message.header.request.bodylen = htonl(8);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpControl,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpControlValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(13);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test observe seqno
class ObserveSeqnoValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    ObserveSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.bodylen = ntohl(8);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ObserveSeqno,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ObserveSeqnoValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 8;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test set drift counter state
class SetDriftCounterStateValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    SetDriftCounterStateValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 9;
        request.message.header.request.bodylen = ntohl(9);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SetDriftCounterState,
                static_cast<void*>(&request));
    }
};

TEST_P(SetDriftCounterStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(19);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetDriftCounterStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test get adjusted time
class GetAdjustedTimeValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    GetAdjustedTimeValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetAdjustedTime,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetAdjustedTimeValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAdjustedTimeValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

enum class RefreshOpcodes : uint8_t {
    Isasl = uint8_t(cb::mcbp::ClientOpcode::IsaslRefresh),
    Ssl = uint8_t(cb::mcbp::ClientOpcode::SslCertsRefresh),
    Rbac = uint8_t(cb::mcbp::ClientOpcode::RbacRefresh)
};

std::string to_string(const RefreshOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case RefreshOpcodes::Isasl:
        return "ISASL";
    case RefreshOpcodes::Ssl:
        return "SSL";
    case RefreshOpcodes::Rbac:
        return "RBAC";
    }
    throw std::invalid_argument("to_string(const RefreshOpcodes&): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const RefreshOpcodes& o) {
    os << to_string(o);
    return os;
}

class RefreshValidatorTest
    : public ::testing::WithParamInterface<std::tuple<RefreshOpcodes, bool>>,
      public ValidatorTest {
public:
    RefreshValidatorTest() : ValidatorTest(std::get<1>(GetParam())) {
    }

protected:
    cb::mcbp::Status validate() {
        auto opcode = (cb::mcbp::ClientOpcode)std::get<0>(GetParam());
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

INSTANTIATE_TEST_CASE_P(
        RefreshOpcodes,
        RefreshValidatorTest,
        ::testing::Combine(::testing::Values(RefreshOpcodes::Isasl,
                                             RefreshOpcodes::Ssl,
                                             RefreshOpcodes::Rbac),
                           ::testing::Bool()),

);

TEST_P(RefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RefreshValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test CmdTimer
class CmdTimerValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    CmdTimerValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetCmdTimer,
                                       static_cast<void*>(&request));
    }
};

TEST_P(CmdTimerValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test GetCtrlToken
class GetCtrlTokenValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    GetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetCtrlToken,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test SetCtrlToken
class SetCtrlTokenValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    SetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.bodylen = htonl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.new_cas = 1;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetCtrlToken,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_set_ctrl_token &request =
        *reinterpret_cast<protocol_binary_request_set_ctrl_token*>(blob);
};

TEST_P(SetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetCtrlTokenValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidNewCas) {
    request.message.body.new_cas = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// cb::mcbp::ClientOpcode::GetAllVbSeqnos
class GetAllVbSeqnoValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    GetAllVbSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_get_all_vb_seqnos &request =
        *reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
};

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageNoState) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageWithState) {
    EXPECT_EQ(4, sizeof(vbucket_state_t));
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    request.message.body.state =
        static_cast<vbucket_state_t>(htonl(vbucket_state_active));
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidVbucketState) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);

    for (int ii = 0; ii < 100; ++ii) {
        request.message.body.state = static_cast<vbucket_state_t>(htonl(ii));
        if (is_valid_vbucket_state_t(static_cast<vbucket_state_t>(ii))) {
            EXPECT_EQ(cb::mcbp::Status::Success, validate());
        } else {
            EXPECT_EQ(cb::mcbp::Status::Einval, validate());
        }
    }
}

// cb::mcbp::ClientOpcode::GetLocked
class GetLockedValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    GetLockedValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetLocked,
                                       static_cast<void*>(&request));
    }
};

TEST_P(GetLockedValidatorTest, CorrectMessageDefaultTimeout) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetLockedValidatorTest, CorrectMessageExplicitTimeout) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(14);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetLockedValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// PROTOCOL_BINARY_CMD_UNLOCK
class UnlockValidatorTest : public ::testing::WithParamInterface<bool>,
                            public ValidatorTest {
public:
    UnlockValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0xdeadbeef;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::UnlockKey,
                                       static_cast<void*>(&request));
    }
};

TEST_P(UnlockValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(UnlockValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test config_reload
class ConfigReloadValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
public:
    ConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ConfigReload,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// cb::mcbp::ClientOpcode::EvictKey
class EvictKeyValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    EvictKeyValidatorTest() : ValidatorTest(GetParam()) {
    }
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::EvictKey,
                                       static_cast<void*>(&request));
    }
};

TEST_P(EvictKeyValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0xff;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class RevokeUserPermissionsValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    RevokeUserPermissionsValidatorTest() : ValidatorTest(GetParam()) {
    }
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0;
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::EvictKey,
                                       static_cast<void*>(&request));
    }
};

TEST_P(RevokeUserPermissionsValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0xff;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, MissingKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class ErrorContextTest : public ::testing::WithParamInterface<bool>,
                         public ValidatorTest {
public:
    ErrorContextTest() : ValidatorTest(GetParam()) {
    }

protected:
    std::string validate_error_context(cb::mcbp::ClientOpcode opcode) {
        void* packet = static_cast<void*>(&request);
        return ValidatorTest::validate_error_context(opcode, packet);
    }
};

TEST_P(ErrorContextTest, ValidHeader) {
    // Error context should not be set on valid request
    EXPECT_EQ("", validate_error_context(cb::mcbp::ClientOpcode::Noop));
}

TEST_P(ErrorContextTest, InvalidHeader) {
    // Magic invalid
    request.message.header.request.magic = 0;
    EXPECT_EQ("Request header invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Extlen + Keylen > Bodylen
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(12);
    EXPECT_EQ("Request header invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(ErrorContextTest, InvalidDatatype) {
    // Nonexistent datatype
    request.message.header.request.datatype = mcbp::datatype::highest + 1;
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Noop command does not accept JSON
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));
}

TEST_P(ErrorContextTest, InvalidExtras) {
    // Noop command does not accept extras
    request.message.header.request.setExtlen(4);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Add command requires extras
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(14);
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(ErrorContextTest, InvalidKey) {
    // Noop command does not accept key
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must not include key",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Add command requires key
    request.message.header.request.setExtlen(8);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(ErrorContextTest, InvalidValue) {
    // Noop command does not accept value
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must not include value",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Create bucket command requires value
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ("Request must include value",
              validate_error_context(cb::mcbp::ClientOpcode::CreateBucket));
}

TEST_P(ErrorContextTest, InvalidCas) {
    // Unlock command requires CAS
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(8);
    request.message.header.request.setBodylen(8);
    request.message.header.request.setCas(0);
    EXPECT_EQ("Request CAS must be set",
              validate_error_context(cb::mcbp::ClientOpcode::UnlockKey));

    // Noop command does not accept CAS
    request.message.header.request.setExtlen(0);
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(0);
    request.message.header.request.setCas(10);
    EXPECT_EQ("Request CAS must not be set",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));
}

class CommandSpecificErrorContextTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    CommandSpecificErrorContextTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        memset(blob, 0, sizeof(blob));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        connection.enableDatatype(cb::mcbp::Feature::XATTR);
        connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
        connection.enableDatatype(cb::mcbp::Feature::JSON);
    }
    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Request& header = request.message.header.request;

    std::string validate_error_context(cb::mcbp::ClientOpcode opcode) {
        void* packet = static_cast<void*>(&request);
        return ValidatorTest::validate_error_context(opcode, packet);
    }
};

TEST_P(CommandSpecificErrorContextTest, DcpOpen) {
    header.setExtlen(8);
    header.setKeylen(10);
    header.setBodylen(20);

    // No value
    EXPECT_EQ("Request must not include value",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));

    // DCP_OPEN_UNUSED flag is invalid
    header.setBodylen(18);
    auto* req = reinterpret_cast<protocol_binary_request_dcp_open*>(blob);
    req->message.body.flags = htonl(DCP_OPEN_UNUSED);
    EXPECT_EQ("Request contains invalid flags",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));

    // DCP_OPEN_NOTIFIER cannot be used in conjunction with other flags
    req->message.body.flags = htonl(DCP_OPEN_NOTIFIER | DCP_OPEN_PRODUCER);
    EXPECT_EQ("Request contains invalid flags combination",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));
}

TEST_P(CommandSpecificErrorContextTest, DcpAddStream) {
    // DCP_ADD_STREAM_FLAG_NO_VALUE is no longer used
    header.setExtlen(4);
    header.setKeylen(0);
    header.setBodylen(4);
    auto* req = reinterpret_cast<protocol_binary_request_dcp_add_stream*>(blob);
    req->message.body.flags = htonl(DCP_ADD_STREAM_FLAG_NO_VALUE);
    EXPECT_EQ("DCP_ADD_STREAM_FLAG_NO_VALUE{8} flag is no longer used",
              validate_error_context(cb::mcbp::ClientOpcode::DcpAddStream));

    // 128 is not a defined flag
    req->message.body.flags = 128;
    EXPECT_EQ("Request contains invalid flags",
              validate_error_context(cb::mcbp::ClientOpcode::DcpAddStream));
}

TEST_P(CommandSpecificErrorContextTest, DcpStreamRequest) {
    header.setExtlen(48);
    header.setKeylen(0);
    header.setBodylen(48 + 10);

    if (isCollectionsEnabled()) {
        EXPECT_EQ("Attached bucket does not support DCP",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
    } else {
        EXPECT_EQ("Request must not include value",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
    }

    header.setKeylen(5);
    header.setBodylen(48 + 5);
    EXPECT_EQ("Request must not include key",
              validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
    header.setKeylen(0);
    header.setBodylen(48);
    header.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
}

TEST_P(CommandSpecificErrorContextTest, DcpSystemEvent) {
    // System event ID must be 0, 1 or 2
    uint8_t extlen =
            protocol_binary_request_dcp_system_event::getExtrasLength();
    header.setExtlen(extlen);
    header.setKeylen(0);
    header.setBodylen(extlen);
    auto* req =
            reinterpret_cast<protocol_binary_request_dcp_system_event*>(blob);
    req->message.body.event = htonl(3);
    EXPECT_EQ("Invalid system event id",
              validate_error_context(cb::mcbp::ClientOpcode::DcpSystemEvent));
}

TEST_P(CommandSpecificErrorContextTest, DcpMutation) {
    // Connection must be Xattr enabled if datatype is Xattr
    uint8_t extlen = protocol_binary_request_dcp_mutation::getExtrasLength();
    header.setExtlen(extlen);
    header.setKeylen(10);
    header.setBodylen(extlen + 10);
    header.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    connection.disableAllDatatypes();
    EXPECT_EQ("Connection not Xattr enabled",
              validate_error_context(cb::mcbp::ClientOpcode::DcpMutation));

    // Request body must be valid Xattr blob if datatype is Xattr
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    EXPECT_EQ("Xattr blob not valid",
              validate_error_context(cb::mcbp::ClientOpcode::DcpMutation));
}

TEST_P(CommandSpecificErrorContextTest, DcpDeletion) {
    // JSON is not a valid datatype for DcpDeletion
    uint8_t extlen = protocol_binary_request_dcp_deletion::extlen;
    header.setExtlen(extlen);
    header.setKeylen(8);
    header.setBodylen(extlen + 8);
    header.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (GetParam()) {
        // Collections enabled - we require a larger message
        EXPECT_EQ("Request must include extras of length 21",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    } else {
        EXPECT_EQ("Request datatype invalid",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    }
}

TEST_P(CommandSpecificErrorContextTest, DcpDeletionV2) {
    // JSON is not a valid datatype for DcpDeletion
    uint8_t extlen = protocol_binary_request_dcp_deletion_v2::extlen;
    header.setExtlen(extlen);
    header.setKeylen(8);
    header.setBodylen(extlen + 8);
    header.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (!GetParam()) {
        // Collections enabled - we require a larger message
        EXPECT_EQ("Request must include extras of length 18",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    } else {
        EXPECT_EQ("Request datatype invalid",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    }
}

TEST_P(CommandSpecificErrorContextTest, DcpSetVbucketState) {
    // Body state must be between 1 and 4
    header.setExtlen(1);
    header.setKeylen(0);
    header.setBodylen(1);
    auto* req =
            reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(
                    blob);
    req->message.body.state = 10;
    EXPECT_EQ(
            "Request body state invalid",
            validate_error_context(cb::mcbp::ClientOpcode::DcpSetVbucketState));
}

TEST_P(CommandSpecificErrorContextTest, Hello) {
    // Hello requires even body length
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(3);
    EXPECT_EQ("Request value must be of even length",
              validate_error_context(cb::mcbp::ClientOpcode::Hello));
}

TEST_P(CommandSpecificErrorContextTest, Flush) {
    // Flush command requires extlen of 0 or 4
    header.setExtlen(3);
    header.setKeylen(0);
    header.setBodylen(3);
    EXPECT_EQ("Request extras must be of length 0 or 4",
              validate_error_context(cb::mcbp::ClientOpcode::Flush));

    // Delayed flush is unsupported
    header.setExtlen(4);
    header.setKeylen(0);
    header.setBodylen(4);
    auto* req = reinterpret_cast<protocol_binary_request_flush*>(blob);
    req->message.body.expiration = 10;
    EXPECT_EQ("Delayed flush no longer supported",
              validate_error_context(cb::mcbp::ClientOpcode::Flush));
}

TEST_P(CommandSpecificErrorContextTest, Add) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(8);
    header.setKeylen(1);
    header.setBodylen(9);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(CommandSpecificErrorContextTest, Set) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(8);
    header.setKeylen(1);
    header.setBodylen(9);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Set));
}

TEST_P(CommandSpecificErrorContextTest, Append) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(2);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Append));
}

TEST_P(CommandSpecificErrorContextTest, Get) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Get));
}

TEST_P(CommandSpecificErrorContextTest, Gat) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(4);
    header.setKeylen(1);
    header.setBodylen(5);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Gat));
}

TEST_P(CommandSpecificErrorContextTest, Delete) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Delete));
}

TEST_P(CommandSpecificErrorContextTest, Increment) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(20);
    header.setKeylen(1);
    header.setBodylen(21);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Increment));
}

TEST_P(CommandSpecificErrorContextTest, SetCtrlToken) {
    // Set Ctrl Token requires new cas
    header.setExtlen(8);
    header.setKeylen(0);
    header.setBodylen(8);
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(blob);
    req->message.body.new_cas = 0;
    EXPECT_EQ("New CAS must be set",
              validate_error_context(cb::mcbp::ClientOpcode::SetCtrlToken));
}

TEST_P(CommandSpecificErrorContextTest, IoctlGet) {
    // Maximum IOCTL_KEY_LENGTH is 128
    header.setExtlen(0);
    header.setKeylen(129);
    header.setBodylen(129);
    EXPECT_EQ("Request key length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::IoctlGet));
}

TEST_P(CommandSpecificErrorContextTest, IoctlSet) {
    // Maximum IOCTL_KEY_LENGTH is 128
    header.setExtlen(0);
    header.setKeylen(129);
    header.setBodylen(129);
    EXPECT_EQ("Request key length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::IoctlSet));

    // Maximum IOTCL_VAL_LENGTH is 128
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(130);
    EXPECT_EQ("Request value length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::IoctlSet));
}

TEST_P(CommandSpecificErrorContextTest, ConfigValidate) {
    // Maximum value length is 65536
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(65537);
    EXPECT_EQ("Request value length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::ConfigValidate));
}

TEST_P(CommandSpecificErrorContextTest, ObserveSeqno) {
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    EXPECT_EQ("Request value must be of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::ObserveSeqno));
}

TEST_P(CommandSpecificErrorContextTest, CreateBucket) {
    // Create Bucket has maximum key length of 100
    header.setExtlen(0);
    header.setKeylen(101);
    header.setBodylen(102);
    EXPECT_EQ("Request key length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::CreateBucket));
}

TEST_P(CommandSpecificErrorContextTest, SelectBucket) {
    // Select Bucket has maximum key length of 1023
    header.setExtlen(0);
    header.setKeylen(1024);
    header.setBodylen(1024);
    EXPECT_EQ("Request key length exceeds maximum",
              validate_error_context(cb::mcbp::ClientOpcode::SelectBucket));
}

TEST_P(CommandSpecificErrorContextTest, GetAllVbSeqnos) {
    // Extlen must be zero or sizeof(vbucket_state_t)
    header.setExtlen(sizeof(vbucket_state_t) + 1);
    header.setKeylen(0);
    header.setBodylen(sizeof(vbucket_state_t) + 1);
    EXPECT_EQ("Request extras must be of length 0 or " +
                      std::to_string(sizeof(vbucket_state_t)),
              validate_error_context(cb::mcbp::ClientOpcode::GetAllVbSeqnos));

    // VBucket state must be between 1 and 4
    header.setExtlen(4);
    header.setKeylen(0);
    header.setBodylen(4);
    auto* req =
            reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
    req->message.body.state = static_cast<vbucket_state_t>(5);
    EXPECT_EQ("Request vbucket state invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetAllVbSeqnos));
}

TEST_P(CommandSpecificErrorContextTest, GetMeta) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));

    // Get Meta requires extlen of 0 or 1
    connection.setCollectionsSupported(false);
    header.setExtlen(2);
    header.setKeylen(4);
    header.setBodylen(6);
    EXPECT_EQ("Request extras must be of length 0 or 1",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));

    // If extlen is 1, then the extras byte must be 1 or 2
    header.setExtlen(1);
    header.setKeylen(4);
    header.setBodylen(5);
    auto* req = reinterpret_cast<protocol_binary_request_get_meta*>(blob);
    uint8_t* extdata = req->bytes + sizeof(req->bytes);
    *extdata = 5;
    EXPECT_EQ("Request extras invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));
}

TEST_P(CommandSpecificErrorContextTest, MutateWithMeta) {
    // Mutate with meta commands must have extlen of 24, 26, 28 or 30
    header.setExtlen(20);
    header.setKeylen(10);
    header.setBodylen(30);
    EXPECT_EQ("Request extras invalid",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));

    // If datatype is Xattr, xattr must be enabled on connection
    header.setExtlen(24);
    header.setKeylen(10);
    header.setBodylen(34);
    header.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    connection.disableAllDatatypes();
    EXPECT_EQ("Connection not Xattr enabled",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));

    // If datatype is Xattr, command value must be valid xattr blob
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    EXPECT_EQ("Xattr blob invalid",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));

    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(24);
    header.setKeylen(1);
    header.setBodylen(25);
    header.datatype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));
}

TEST_P(CommandSpecificErrorContextTest, GetErrmap) {
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    EXPECT_EQ("Request value must be of length 2",
              validate_error_context(cb::mcbp::ClientOpcode::GetErrorMap));

    // Get Errmap command requires vbucket id 0
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(2);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(cb::mcbp::ClientOpcode::GetErrorMap));
}

TEST_P(CommandSpecificErrorContextTest, GetLocked) {
    header.setExtlen(2);
    header.setKeylen(8);
    header.setBodylen(10);
    EXPECT_EQ("Request extras must be of length 0 or 4",
              validate_error_context(cb::mcbp::ClientOpcode::GetLocked));

    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetLocked));
}

TEST_P(CommandSpecificErrorContextTest, UnlockKey) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    header.setCas(10);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::UnlockKey));
}

TEST_P(CommandSpecificErrorContextTest, EvictKey) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Request key invalid",
              validate_error_context(cb::mcbp::ClientOpcode::EvictKey));
}

TEST_P(CommandSpecificErrorContextTest, CollectionsSetManifest) {
    // VBucket ID must not be set
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(
                      cb::mcbp::ClientOpcode::CollectionsSetManifest));

    // Attached bucket must support collections
    header.setVBucket(Vbid(0));
    EXPECT_EQ("Attached bucket does not support collections",
              validate_error_context(
                      cb::mcbp::ClientOpcode::CollectionsSetManifest));
}

TEST_P(CommandSpecificErrorContextTest, CollectionsGetManifest) {
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(0);
    EXPECT_EQ("Attached bucket does not support collections",
              validate_error_context(
                      cb::mcbp::ClientOpcode::CollectionsGetManifest));
}

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AddValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetReplaceValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AppendPrependValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DeleteValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IncrementDecrementValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        QuitValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        FlushValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        NoopValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        VersionValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        StatValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        VerbosityValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        HelloValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SaslListMechValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SaslAuthValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetErrmapValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IoctlGetValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        IoctlSetValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AuditPutValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        AuditConfigReloadValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ShutdownValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpOpenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpAddStreamValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpCloseStreamValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpGetFailoverLogValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpStreamReqValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpStreamEndValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpSnapshotMarkerValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpMutationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpDeletionValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpExpirationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpSetVbucketStateValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpNoopValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpBufferAckValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpControlValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ObserveSeqnoValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetDriftCounterStateValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetAdjustedTimeValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        CmdTimerValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetCtrlTokenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetCtrlTokenValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetAllVbSeqnoValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetLockedValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        UnlockValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ConfigReloadValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        EvictKeyValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        RevokeUserPermissionsValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ErrorContextTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());
INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        CommandSpecificErrorContextTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

} // namespace test
} // namespace mcbp

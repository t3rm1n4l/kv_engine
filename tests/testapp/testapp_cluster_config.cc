/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <cctype>
#include <limits>
#include <thread>
#include "testapp_arithmetic.h"

class ClusterConfigTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        // Make sure we've specified a session token
        setClusterSessionToken(0xdeadbeef);
    }

    BinprotResponse setClusterConfig(uint64_t token,
                                     const std::string& config) {
        auto& conn = getAdminConnection();
        conn.selectBucket("default");
        BinprotResponse response;
        conn.executeCommand(BinprotSetClusterConfigCommand{token, config},
                            response);
        return response;
    }

    void test_MB_17506(bool dedupe);
};

void ClusterConfigTest::test_MB_17506(bool dedupe) {
    // First set the correct deduplication mode
    memcached_cfg["dedupe_nmvb_maps"] = dedupe;
    reconfigure();

    const std::string clustermap{R"({"rev":100})"};

    // Make sure we have a cluster configuration installed
    auto response = setClusterConfig(token, clustermap);
    EXPECT_TRUE(response.isSuccess());

    auto& conn = getConnection();
    BinprotGetCommand command;
    command.setKey("foo");
    command.setVBucket(Vbid(1));

    // Execute the first get command. This one should _ALWAYS_ contain a map
    conn.executeCommand(command, response);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::NotMyVbucket, response.getStatus());
    EXPECT_EQ(clustermap, response.getDataString());

    // Execute it one more time..
    conn.executeCommand(command, response);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::NotMyVbucket, response.getStatus());

    if (dedupe) {
        EXPECT_TRUE(response.getDataString().empty())
                << "Expected an empty stream, got [" << response.getDataString()
                << "]";
    } else {
        EXPECT_EQ(clustermap, response.getDataString());
    }
}

INSTANTIATE_TEST_CASE_P(
        TransportProtocols,
        ClusterConfigTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(ClusterConfigTest, SetClusterConfigWithIncorrectSessionToken) {
    auto response = setClusterConfig(0xcafebeef, R"({"rev":100})");
    EXPECT_FALSE(response.isSuccess()) << "Should not be allowed to set "
                                          "cluster config with invalid session "
                                          "token";
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, response.getStatus());
}

TEST_P(ClusterConfigTest, SetClusterConfigWithCorrectTokenInvalidPayload) {
    auto response = setClusterConfig(token, R"({"foo":"bar"})");
    EXPECT_FALSE(response.isSuccess())
            << "Should not be allowed to set cluster config invalid payload";
    EXPECT_EQ(cb::mcbp::Status::Einval, response.getStatus());
}

TEST_P(ClusterConfigTest, SetClusterConfigWithCorrectToken) {
    auto response = setClusterConfig(token, R"({"rev":100})");
    EXPECT_TRUE(response.isSuccess()) << "Should be allowed to set cluster "
                                         "config with the correct session "
                                         "token";
}

TEST_P(ClusterConfigTest, GetClusterConfig) {
    const std::string config{R"({"rev":100})"};
    auto response = setClusterConfig(token, config);
    ASSERT_TRUE(response.isSuccess());

    BinprotGenericCommand cmd{PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, "", ""};
    auto& conn = getConnection();
    conn.executeCommand(cmd, response);
    EXPECT_TRUE(response.isSuccess());
    const auto value = response.getDataString();
    EXPECT_EQ(config, value);
    EXPECT_TRUE(hasCorrectDatatype(expectedJSONDatatype(),
                                   cb::mcbp::Datatype(response.getDatatype()),
                                   {value.data(), value.size()}));
}

TEST_P(ClusterConfigTest, test_MB_17506_no_dedupe) {
    test_MB_17506(false);
}

TEST_P(ClusterConfigTest, test_MB_17506_dedupe) {
    test_MB_17506(true);
}

TEST_P(ClusterConfigTest, Enable_CCCP_Push_Notifications) {
    auto& conn = getConnection();
    // The "connection class" ignore the context part in
    // extended error message unless we enable the JSON datatype
    conn.setDatatypeJson(true);

    conn.setClustermapChangeNotification(false);
    conn.setDuplexSupport(false);

    try {
        conn.setClustermapChangeNotification(true);
        FAIL() << "It should not be possible to enable CCCP push notifications "
                  "without duplex";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(
                "Failed to say hello: 'Clustermap change notification needs "
                "Duplex', Invalid arguments (4)",
                e.what());
    }

    // With duplex we should we good to go
    conn.setDuplexSupport(true);
    conn.setClustermapChangeNotification(true);
}

TEST_P(ClusterConfigTest, CccpPushNotification) {
    auto& conn = getAdminConnection();
    conn.selectBucket("default");

    auto second = conn.clone();

    second->setDuplexSupport(true);
    second->setClustermapChangeNotification(true);

    BinprotResponse response;
    conn.executeCommand(BinprotSetClusterConfigCommand{token, R"({"rev":666})"},
                        response);

    Frame frame;

    // Setting a new config should cause the server to push a new config
    // to me!
    second->recvFrame(frame);
    EXPECT_EQ(cb::mcbp::Magic::ServerRequest, frame.getMagic());

    auto* request = frame.getRequest();

    EXPECT_EQ(cb::mcbp::ServerOpcode::ClustermapChangeNotification,
              request->getServerOpcode());
    EXPECT_EQ(4, request->getExtlen());
    auto extras = request->getExtdata();
    uint32_t revno;
    std::copy(extras.begin(), extras.end(), reinterpret_cast<uint8_t*>(&revno));
    revno = ntohl(revno);
    EXPECT_EQ(666, revno);

    auto key = request->getKey();
    const std::string bucket{reinterpret_cast<const char*>(key.data()),
                             key.size()};
    EXPECT_EQ("default", bucket);

    auto value = request->getValue();
    const std::string config{reinterpret_cast<const char*>(value.data()),
                             value.size()};
    EXPECT_EQ(R"({"rev":666})", config);
}

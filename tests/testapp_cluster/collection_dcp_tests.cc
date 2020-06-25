/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "clustertest.h"

#include "auth_provider_service.h"
#include "bucket.h"
#include "cluster.h"
#include <include/mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class CollectionsDcpTests : public cb::test::ClusterTest {
protected:
    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->setFeature(cb::mcbp::Feature::Collections, true);
        return conn;
    }
};

// Setup will add a user that can do all-bucket streams
class CollectionsRbacBucket : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the entire bucket
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer", "DcpStream"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
        // Load a user that has DcpProducer privs for the entire bucket
        // (cannot stream)
        cluster->getAuthProviderService().upsertUser(
                {usernameNoStream, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacBucket");
        conn->dcpControl("enable_noop", "true");

        connNoStream = getConnection();
        connNoStream->authenticate(usernameNoStream, password);
        connNoStream->selectBucket("default");
        connNoStream->dcpOpenProducer("CollectionsRbacBucketNoStream");
        connNoStream->dcpControl("enable_noop", "true");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        cluster->getAuthProviderService().removeUser(usernameNoStream);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    std::unique_ptr<MemcachedConnection> connNoStream;

    const std::string username{"CollectionsRbacBucket"};
    const std::string usernameNoStream{"CollectionsRbacBucketNoStream"};
    const std::string password{"CollectionsRbacBucket"};
};

TEST_F(CollectionsRbacBucket, BucketAccessBucketSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
}

TEST_F(CollectionsRbacBucket, BucketAccessScopeSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessCollection2Success) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessFail) {
    try {
        connNoStream->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
    try {
        connNoStream->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
        FAIL() << "Expected a throw";
    } catch (const ConnectionError& e) {
        // No access and 0 privs == unknown scope
        EXPECT_TRUE(e.isUnknownScope());
        EXPECT_EQ(cluster->collections.getUidString(),
                  e.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }
    try {
        connNoStream->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
        FAIL() << "Expected a throw";
    } catch (const ConnectionError& e) {
        // No access and 0 privs == unknown collection
        EXPECT_TRUE(e.isUnknownCollection());
        EXPECT_EQ(cluster->collections.getUidString(),
                  e.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }
}

// Setup will add a user that can do scope:0 streams
class CollectionsRbacScope : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the scope:0 only
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": ["DcpProducer"],
      "scopes": {
        "0": {
          "privileges": [
            "DcpStream"
          ]
        }
      }
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacScope");
        conn->dcpControl("enable_noop", "true");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    const std::string username{"CollectionsRbacScope"};
    const std::string password{"CollectionsRbacScope"};
};

TEST_F(CollectionsRbacScope, ScopeAccessBucketEaccess) {
    try {
        conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacScope, ScopeAccessScopeSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
}

TEST_F(CollectionsRbacScope, ScopeAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
}

TEST_F(CollectionsRbacScope, ScopeAccessCollection2Success) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
}

// Setup will add a user that can do collection:0 streams
class CollectionsRbacCollection : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the collection:0
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ],
      "scopes": {
        "0": {
          "collections": {
            "0": {
              "privileges": [
                "DcpStream"
              ]
            }
          }
        },
        "8": {
          "collections": {
            "10": {
              "privileges": [
                "Upsert"
              ]
            }
          }
        }
      }
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacCollection");
        conn->dcpControl("enable_noop", "true");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    const std::string username{"CollectionsRbacCollection"};
    const std::string password{"CollectionsRbacCollection"};
};

TEST_F(CollectionsRbacCollection, CollectionAccessBucketEaccess) {
    try {
        conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessScopeEaccess) {
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionUnknown1) {
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isUnknownCollection());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionUnknown2) {
    try {
        // Even though we can see collection 0, this test checks we get unknown
        // collection because of the 0 privs on collection 9
        conn->dcpStreamRequest(Vbid(0),
                               0,
                               0,
                               ~0,
                               0,
                               0,
                               0,
                               R"({"collections":["0", "9", "a"]})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isUnknownCollection());
    }
}

TEST_F(CollectionsDcpTests, TestBasicRbacCollectionsSuccess) {
    const std::string username{"TestBasicRbacCollectionsSuccess"};
    const std::string password{"TestBasicRbacCollectionsSuccess"};
    cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer", "DcpStream"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

    auto conn = getConnection();
    conn->authenticate(username, password);
    conn->selectBucket("default");
    conn->dcpOpenProducer("TestBasicRbacCollectionsSuccess");
    conn->dcpControl("enable_noop", "true");
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
    cluster->getAuthProviderService().removeUser(username);
}

TEST_F(CollectionsDcpTests, TestBasicRbacFail) {
    const std::string username{"TestBasicRbacFail"};
    const std::string password{"TestBasicRbacFail"};
    cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

    auto conn = getConnection();
    conn->authenticate(username, password);
    conn->selectBucket("default");
    conn->dcpOpenProducer("TestBasicRbacFail");
    conn->dcpControl("enable_noop", "true");
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
        FAIL() << "Expected dcpStreamRequest to throw";
    } catch (const ConnectionError& error) {
        // No privs associated with the collection, so it's unknown
        EXPECT_TRUE(error.isUnknownCollection())
                << to_string(error.getReason());
    }
    cluster->getAuthProviderService().removeUser(username);
}

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

#pragma once

#include "kvstore_config.h"
#include "libmagma/magma.h"

class Configuration;

// This class represents the MagmaKVStore specific configuration.
// MagmaKVStore uses this in place of the KVStoreConfig base class.
class MagmaKVStoreConfig : public KVStoreConfig {
public:
    // Initialize the object from the central EPEngine Configuration
    MagmaKVStoreConfig(Configuration& config, uint16_t shardid);

    size_t getBucketQuota() {
        return bucketQuota;
    }
    size_t getMagmaLsdBufferSize() const {
        return magmaLsdBufferSize;
    }
    float getMagmaLsdFragmentationRatio() const {
        return magmaLsdFragmentationRatio;
    }
    int getMagmaMaxCommitPoints() const {
        return magmaMaxCommitPoints;
    }
    int getMagmaCommitPointInterval() const {
        return magmaCommitPointInterval;
    }
    size_t getMagmaMinValueSize() const {
        return magmaMinValueSize;
    }
    size_t getMagmaMaxWriteCache() const {
        return magmaMaxWriteCache;
    }
    size_t getMagmaMinWriteCache() const {
        return magmaMinWriteCache;
    }
    float getMagmaMemQuotaRatio() const {
        return magmaMemQuotaRatio;
    }
    size_t getMagmaWalBufferSize() const {
        return magmaWalBufferSize;
    }
    int getMagmaNumFlushers() const {
        return magmaNumFlushers;
    }
    int getMagmaNumCompactors() const {
        return magmaNumCompactors;
    }
    int getMagmaWalSyncInterval() const {
        return magmaWalSyncInterval;
    }
    bool getMagmaBatchCommitPoint() const {
        return magmaBatchCommitPoint;
    }
    bool getUseUpsert() const {
        return magmaUseUpsert;
    }
    float getMagmaExpiryFragThreshold() const {
        return magmaExpiryFragThreshold;
    }
    float getMagmaTombstoneFragThreshold() const {
        return magmaTombstoneFragThreshold;
    }

    Magma::Config cfg;

private:
    size_t bucketQuota;
    size_t magmaLsdBufferSize;
    float magmaLsdFragmentationRatio;
    int magmaMaxCommitPoints;
    int magmaCommitPointInterval;
    size_t magmaMinValueSize;
    size_t magmaMaxWriteCache;
    size_t magmaMinWriteCache;
    float magmaMemQuotaRatio;
    size_t magmaWalBufferSize;
    int magmaNumFlushers;
    int magmaNumCompactors;
    int magmaWalSyncInterval;
    bool magmaBatchCommitPoint;
    bool magmaUseUpsert;
    float magmaExpiryFragThreshold;
    float magmaTombstoneFragThreshold;
};

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

#include "magma-kvstore_config.h"

MagmaKVStoreConfig::MagmaKVStoreConfig(Configuration& config, uint16_t shardid)
    : KVStoreConfig(config, shardid) {
    bucketQuota = config.getMaxSize();
    magmaLsdBufferSize = config.getMagmaLsdBufferSize();
    magmaLsdFragmentationRatio = config.getMagmaLsdFragmentationRatio();
    magmaMaxCommitPoints = config.getMagmaMaxCommitPoints();
    magmaCommitPointInterval = config.getMagmaCommitPointInterval();
    magmaMinValueSize = config.getMagmaMinValueSize();
    magmaMaxWriteCache = config.getMagmaMaxWriteCache();
    magmaMinWriteCache = config.getMagmaMinWriteCache();
    magmaMemQuotaRatio = config.getMagmaMemQuotaRatio();
    magmaWalBufferSize = config.getMagmaWalBufferSize();
    magmaNumFlushers = config.getMagmaNumFlushers();
    magmaNumCompactors = config.getMagmaNumCompactors();
    magmaWalSyncInterval = config.getMagmaWalSyncInterval();
    magmaBatchCommitPoint = config.isMagmaBatchCommitPoint();
    magmaUseUpsert = config.isMagmaUseUpsert();
    magmaExpiryFragThreshold = config.getMagmaExpiryFragThreshold();
    magmaTombstoneFragThreshold = config.getMagmaTombstoneFragThreshold();
}

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

#include <sstream>
#include <string>

namespace Collections {
namespace VB {
/**
 * The collection stats that we persist on disk. Provides encoding and
 * decoding of stats.
 */
struct PersistedStats {
    PersistedStats() : itemCount(0), highSeqno(0) {
    }

    PersistedStats(uint64_t itemCount, uint64_t highSeqno)
        : itemCount(itemCount), highSeqno(highSeqno) {
    }

    /// Build from a buffer containing a LEB 128 encoded string
    PersistedStats(const char* buf, size_t size);

    /**
     * @return a LEB 128 encoded version of these stats ready for
     *         persistence
     */
    std::string getLebEncodedStats();

    uint64_t itemCount;
    uint64_t highSeqno;

    std::string to_string() {
        std::stringstream ss;
        ss << "itemCount:" << itemCount << " highSeqno:" << highSeqno;
        return ss.str();
    }
};
} // end namespace VB
} // end namespace Collections

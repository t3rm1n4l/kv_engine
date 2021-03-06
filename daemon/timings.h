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
#pragma once

#include "timing_histogram.h"
#include "timing_interval.h"

#include <array>
#include <string>
#include <mutex>


#define MAX_NUM_OPCODES 0x100

/** Records timings for each memcached opcode. Each opcode has a histogram of
 * times.
 */
class Timings {
public:
    Timings();
    Timings& operator=(const Timings& other);
    Timings(const Timings&) = delete;

    void reset();
    void collect(uint8_t opcode, std::chrono::nanoseconds nsec);
    void sample(std::chrono::seconds sample_interval);
    std::string generate(uint8_t opcode);
    uint64_t get_aggregated_mutation_stats();
    uint64_t get_aggregated_retrival_stats();

    cb::sampling::Interval get_interval_mutation_latency();
    cb::sampling::Interval get_interval_lookup_latency();

    /**
     * Get the underlying timings histogram for the specified opcode
     */
    TimingHistogram get_timing_histogram(uint8_t opcode) const {
        return timings[opcode];
    }

private:
    // This lock is only held by sample() and some blocks within generate().
    // It guards the various IntervalSeries variables which internally
    // contain cb::RingBuffer objects which are not thread safe.
    std::mutex lock;

    cb::sampling::IntervalSeries interval_latency_lookups;
    cb::sampling::IntervalSeries interval_latency_mutations;
    std::array<TimingHistogram, MAX_NUM_OPCODES> timings;
    std::array<cb::sampling::Interval, MAX_NUM_OPCODES> interval_counters;
};

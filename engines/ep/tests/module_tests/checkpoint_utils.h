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

#include "checkpoint.h"

/**
 * Stateless class used to gain privileged access into CheckpointCursor for
 * testing purposes. This is a friend class of CheckpointCursor.
 */
class CheckpointCursorIntrospector {
public:
    static const CheckpointQueue::iterator& getCurrentPos(
            const CheckpointManager& cm, const std::string& cursorName) {
        auto& cursor = cm.connCursors.find(cursorName)->second;
        return cursor.currentPos;
    }
};
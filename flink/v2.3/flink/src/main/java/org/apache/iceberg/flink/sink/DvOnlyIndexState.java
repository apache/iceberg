/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.Serializable;

/**
 * What {@link DvOnlyCoordinator} keeps across restarts.
 *
 * @param built whether every data file of the branch was requested for indexing at least once
 * @param baselineSnapshotId snapshot the index reflects, or null while the branch has none
 * @param generation generation the index was last rebuilt for
 * @param committedCheckpointId last checkpoint of this sink seen committed to the branch
 * @param keyFingerprint identifies the equality fields the index is keyed by
 */
record DvOnlyIndexState(
    boolean built,
    Long baselineSnapshotId,
    long generation,
    long committedCheckpointId,
    String keyFingerprint)
    implements Serializable {}

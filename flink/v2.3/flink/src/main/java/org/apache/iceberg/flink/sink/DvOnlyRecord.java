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
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;

@Internal
public record DvOnlyRecord(
    Type type,
    SerializedEqualityValues key,
    DVPosition position,
    String filePath,
    byte[] readTask,
    long checkpointId,
    long generation)
    implements Serializable {

  /** Used by records that do not belong to a checkpoint of this job. */
  public static final long NO_CHECKPOINT = -1L;

  /** Used by records that are not tied to an index generation. */
  public static final long NO_GENERATION = -1L;

  public enum Type {
    /** Delete of a key that no row written in the same checkpoint matched. */
    DELETE,
    /** Row written during the current checkpoint, indexed once that checkpoint resolves. */
    ADD_ROW,
    /** Row the table already holds, indexed right away. */
    BOOTSTRAP_ROW,
    /** Drops the positions a key holds in one data file, because that file left the table. */
    DROP_POSITIONS,
    /** Asks for the live rows of one data file to be reported as {@link #BOOTSTRAP_ROW}. */
    READ_FILE,
    /** Reports that a data file left the table, so the keys it held have to be dropped. */
    DROP_FILE,
    /**
     * Reports that the index was rebuilt from the whole branch, so every data file that neither was
     * read for this generation nor holds rows of an uncommitted checkpoint has to be dropped.
     */
    CLEANUP
  }

  public static DvOnlyRecord delete(SerializedEqualityValues key, long checkpointId) {
    return new DvOnlyRecord(Type.DELETE, key, null, null, null, checkpointId, NO_GENERATION);
  }

  public static DvOnlyRecord addRow(PkIndexEntry entry, long checkpointId) {
    return new DvOnlyRecord(
        Type.ADD_ROW,
        entry.key(),
        entry.position(),
        entry.position().dataFilePath(),
        null,
        checkpointId,
        NO_GENERATION);
  }

  public static DvOnlyRecord bootstrapRow(PkIndexEntry entry) {
    return new DvOnlyRecord(
        Type.BOOTSTRAP_ROW,
        entry.key(),
        entry.position(),
        entry.position().dataFilePath(),
        null,
        NO_CHECKPOINT,
        NO_GENERATION);
  }

  public static DvOnlyRecord dropPositions(SerializedEqualityValues key, String filePath) {
    return new DvOnlyRecord(
        Type.DROP_POSITIONS, key, null, filePath, null, NO_CHECKPOINT, NO_GENERATION);
  }

  public static DvOnlyRecord readFile(String filePath, byte[] readTask, long generation) {
    return new DvOnlyRecord(
        Type.READ_FILE, null, null, filePath, readTask, NO_CHECKPOINT, generation);
  }

  public static DvOnlyRecord dropFile(String filePath) {
    return new DvOnlyRecord(
        Type.DROP_FILE, null, null, filePath, null, NO_CHECKPOINT, NO_GENERATION);
  }

  public static DvOnlyRecord cleanup(long generation, long committedCheckpointId) {
    return new DvOnlyRecord(
        Type.CLEANUP, null, null, null, null, committedCheckpointId, generation);
  }
}

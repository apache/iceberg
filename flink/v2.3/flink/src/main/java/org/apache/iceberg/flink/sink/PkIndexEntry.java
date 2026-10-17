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

/**
 * The location of a live row, keyed by its equality field values. Emitted by the writer for rows it
 * wrote and by the bootstrap reader for rows already in the table, and consumed by the operator
 * that resolves equality deletes to deletion vectors.
 *
 * @param key serialized equality field values of the row
 * @param position file, offset, spec and partition of the row
 */
@Internal
public record PkIndexEntry(SerializedEqualityValues key, DVPosition position)
    implements Serializable {

  /**
   * Data sequence number of every position in the DV-only write path, whether its data file is
   * committed or not. Deletes there are ordered by checkpoint rather than by sequence number.
   */
  public static final long UNKNOWN_SEQUENCE = -1L;
}

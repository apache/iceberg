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
package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

public class GenericDeleteFile extends BaseFile<DeleteFile> implements DeleteFile {
  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  GenericDeleteFile(Schema avroSchema) {
    super(avroSchema);
  }

  /** Used by internal readers to instantiate this class with a projection schema. */
  GenericDeleteFile(Types.StructType projection) {
    super(projection);
  }

  public GenericDeleteFile(
      int specId,
      FileContent content,
      String filePath,
      FileFormat format,
      PartitionData partition,
      long fileSizeInBytes,
      Metrics metrics,
      int[] equalityFieldIds,
      Integer sortOrderId,
      List<Long> splitOffsets,
      ByteBuffer keyMetadata) {
    super(
        specId,
        content,
        filePath,
        format,
        partition,
        fileSizeInBytes,
        metrics.recordCount(),
        metrics.columnSizes(),
        metrics.valueCounts(),
        metrics.nullValueCounts(),
        metrics.nanValueCounts(),
        metrics.lowerBounds(),
        metrics.upperBounds(),
        splitOffsets,
        equalityFieldIds,
        sortOrderId,
        keyMetadata);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   * @param copyStats whether to copy all fields or to drop column-level stats.
   * @param requestedColumnIds column ids for which to keep stats. If <code>null</code> then every
   *     column stat is kept.
   */
  private GenericDeleteFile(
      GenericDeleteFile toCopy, boolean copyStats, Set<Integer> requestedColumnIds) {
    super(toCopy, copyStats, requestedColumnIds);
  }

  /** Constructor for Java serialization. */
  GenericDeleteFile() {}

  @Override
  public DeleteFile copyWithoutStats() {
    return new GenericDeleteFile(this, false /* drop stats */, null);
  }

  @Override
  public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
    return new GenericDeleteFile(this, true, requestedColumnIds);
  }

  @Override
  public DeleteFile copy() {
    return new GenericDeleteFile(this, true /* full copy */, null);
  }

  @Override
  protected Schema getAvroSchema(Types.StructType partitionStruct) {
    Types.StructType type = DataFile.getType(partitionStruct);
    return AvroSchemaUtil.convert(
        type,
        ImmutableMap.of(
            type, GenericDeleteFile.class.getName(),
            partitionStruct, PartitionData.class.getName()));
  }
}

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
package org.apache.iceberg.spark.source;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

class DVIterator implements CloseableIterator<InternalRow> {
  private final PartitionSpec spec;
  private final DeleteFile deleteFile;
  private final Schema projection;
  private final Map<Integer, ?> idToConstant;
  private final Iterator<Long> positions;
  private Integer deletedPositionIndex;
  private GenericInternalRow row;

  DVIterator(
      InputFile inputFile,
      DeleteFile deleteFile,
      PartitionSpec spec,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    this.deleteFile = deleteFile;
    this.spec = spec;
    this.projection = projection;
    this.idToConstant = idToConstant;
    List<Long> pos = Lists.newArrayList();
    new BaseDeleteLoader(ignored -> inputFile)
        .loadPositionDeletes(ImmutableList.of(deleteFile), deleteFile.referencedDataFile())
        .forEach(pos::add);
    this.positions = pos.iterator();
  }

  @Override
  public boolean hasNext() {
    return positions.hasNext();
  }

  @Override
  public InternalRow next() {
    long position = positions.next();

    if (null == row) {
      List<Object> rowValues = Lists.newArrayList();
      if (null != projection.findField(MetadataColumns.DELETE_FILE_PATH.fieldId())) {
        rowValues.add(UTF8String.fromString(deleteFile.referencedDataFile()));
      }

      if (null != projection.findField(MetadataColumns.DELETE_FILE_POS.fieldId())) {
        rowValues.add(position);
        // remember the index where the deleted position needs to be set
        deletedPositionIndex = rowValues.size() - 1;
      }

      Types.NestedField partition = projection.findField(MetadataColumns.PARTITION_COLUMN_ID);
      if (null != partition) {
        Object constant = idToConstant.get(MetadataColumns.PARTITION_COLUMN_ID);
        if (null != constant) {
          rowValues.add(constant);
        } else {
          Types.StructType type = partition.type().asStructType();
          StructInternalRow partitionRow = new StructInternalRow(type);
          StructLike copiedPartition = StructLikeUtil.copy(deleteFile.partition());
          partitionRow.setStruct(PartitionUtil.coercePartition(type, spec, copiedPartition));
          rowValues.add(partitionRow);
        }
      }

      if (null != projection.findField(MetadataColumns.SPEC_ID_COLUMN_ID)) {
        Object constant = idToConstant.get(MetadataColumns.SPEC_ID_COLUMN_ID);
        rowValues.add(null != constant ? constant : deleteFile.specId());
      }

      if (null != projection.findField(MetadataColumns.FILE_PATH_COLUMN_ID)) {
        Object constant = idToConstant.get(MetadataColumns.FILE_PATH_COLUMN_ID);
        rowValues.add(null != constant ? constant : UTF8String.fromString(deleteFile.location()));
      }

      this.row = new GenericInternalRow(rowValues.toArray());
    } else if (null != deletedPositionIndex) {
      // only update the deleted position if necessary, everything else stays the same
      row.update(deletedPositionIndex, position);
    }

    return row;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported");
  }

  @Override
  public void close() {}
}

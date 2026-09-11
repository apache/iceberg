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
package org.apache.iceberg.parquet.metadata;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/** Utilities for converting between Iceberg and Hadoop Parquet metadata. */
public class ParquetMetadataUtil {

  private ParquetMetadataUtil() {}

  /** Convert Hadoop ParquetMetadata to native ParquetMetadata. */
  public static ParquetMetadata fromHadoopMetadata(
      org.apache.parquet.hadoop.metadata.ParquetMetadata hadoopMetadata) {
    org.apache.iceberg.parquet.metadata.FileMetadata fileMetadata =
        new org.apache.iceberg.parquet.metadata.FileMetadata(
            hadoopMetadata.getFileMetaData().getSchema(),
            hadoopMetadata.getFileMetaData().getKeyValueMetaData(),
            hadoopMetadata.getFileMetaData().getCreatedBy());

    List<BlockMetadata> blocks = Lists.newArrayList();
    for (org.apache.parquet.hadoop.metadata.BlockMetaData block : hadoopMetadata.getBlocks()) {
      List<ColumnChunkMetadata> columns = Lists.newArrayList();
      for (ColumnChunkMetaData column : block.getColumns()) {
        columns.add(
            ColumnChunkMetadata.get(
                column.getPath(),
                column.getPrimitiveType(),
                column.getCodec(),
                column.getEncodingStats(),
                column.getEncodings(),
                column.getStatistics(),
                column.getFirstDataPageOffset(),
                column.getDictionaryPageOffset(),
                column.getValueCount(),
                column.getTotalSize(),
                column.getTotalUncompressedSize()));
      }

      blocks.add(new BlockMetadata(block.getRowCount(), columns));
    }

    return new ParquetMetadata(fileMetadata, blocks);
  }
}

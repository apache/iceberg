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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

@Internal
interface StreamingManifestWriter {
  ManifestFile close() throws IOException;

  ManifestFile write(RowData rowData);

  List<RowData> pending();

  class Builder implements Serializable {
    private final FileIO io;
    private final PartitionSpec spec;

    private final long targetManifestSizeBytes;
    private final Map<String, Integer> positions;
    private final Types.StructType combinedFileType;
    private final Types.StructType fileType;
    private final RowType rowType;
    private final int formatVersion;
    private final String outputLocation;
    private final Integer rowsDivisor;

    Builder(
        Table table,
        String outputLocation,
        int formatVersion,
        RowType entriesTableType,
        Map<String, Integer> positions,
        long targetManifestSizeBytes,
        Integer rowsDivisor) {
      this.io = table.io();
      this.spec = table.spec();
      this.outputLocation = outputLocation;
      this.formatVersion = formatVersion;
      this.positions = positions;
      this.targetManifestSizeBytes = targetManifestSizeBytes;
      this.rowsDivisor = rowsDivisor;

      this.combinedFileType = DataFile.getType(Partitioning.partitionType(table));
      this.fileType = DataFile.getType(table.spec().partitionType());
      this.rowType = (RowType) entriesTableType.getTypeAt(positions.get(WriteManifests.DATA_FILE));
    }

    StreamingManifestWriter build(ManifestContent content) {
      ManifestWriterFactory factory =
          new ManifestWriterFactory(
              io, spec, formatVersion, outputLocation, targetManifestSizeBytes, rowsDivisor);
      switch (content) {
        case DATA:
          return new ManifestWriter<>(
              factory.newRollingManifestWriter(),
              new FlinkDataFile(combinedFileType, fileType, rowType),
              positions,
              rowType.getFieldCount());
        case DELETES:
          return new ManifestWriter<>(
              factory.newRollingDeleteManifestWriter(),
              new FlinkDeleteFile(combinedFileType, fileType, rowType),
              positions,
              rowType.getFieldCount());
        default:
          throw new IllegalArgumentException("Unknown content type: " + content);
      }
    }
  }

  class ManifestWriter<T extends ContentFile<T>> implements StreamingManifestWriter {
    private final Map<String, Integer> positions;
    private final int dataFileFieldCount;
    private final List<RowData> pending;
    private final StreamingRollingManifestWriter<T> rollingWriter;
    private final FlinkContentFile<T> wrapper;

    ManifestWriter(
        StreamingRollingManifestWriter<T> rollingWriter,
        FlinkContentFile<T> wrapper,
        Map<String, Integer> positions,
        int dataFileFieldCount) {
      this.rollingWriter = rollingWriter;
      this.wrapper = wrapper;
      this.positions = positions;
      this.dataFileFieldCount = dataFileFieldCount;
      this.pending = Lists.newArrayList();
    }

    private ManifestFile closeInternalWriter() throws IOException {
      return rollingWriter.closeWithResult();
    }

    private ManifestFile writeToInternalWriter(RowData rowData) {
      T contentFile =
          wrapper.wrap(rowData.getRow(positions.get(WriteManifests.DATA_FILE), dataFileFieldCount));
      return rollingWriter.existingWithResult(
          contentFile,
          rowData.getLong(positions.get(WriteManifests.SNAPSHOT_ID)),
          rowData.getLong(positions.get(WriteManifests.SEQUENCE_NUMBER)),
          rowData.getLong(positions.get(WriteManifests.FILE_SEQUENCE_NUMBER)));
    }

    @Override
    public ManifestFile close() throws IOException {
      return closeInternalWriter();
    }

    @Override
    public ManifestFile write(RowData rowData) {
      ManifestFile newManifest = writeToInternalWriter(rowData);
      if (newManifest != null) {
        pending.clear();
      }

      pending.add(rowData);
      return newManifest;
    }

    @Override
    public List<RowData> pending() {
      return pending;
    }
  }
}

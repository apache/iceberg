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
package org.apache.iceberg.io;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.StructLikeSet;

public abstract class WriterTestBase<T> extends TableTestBase {

  public WriterTestBase(int formatVersion) {
    super(formatVersion);
  }

  protected abstract FileWriterFactory<T> newWriterFactory(
      Schema dataSchema,
      List<Integer> equalityFieldIds,
      Schema equalityDeleteRowSchema,
      Schema positionDeleteRowSchema);

  protected FileWriterFactory<T> newWriterFactory(
      Schema dataSchema, List<Integer> equalityFieldIds, Schema equalityDeleteRowSchema) {
    return newWriterFactory(dataSchema, equalityFieldIds, equalityDeleteRowSchema, null);
  }

  protected FileWriterFactory<T> newWriterFactory(
      Schema dataSchema, Schema positionDeleteRowSchema) {
    return newWriterFactory(dataSchema, null, null, positionDeleteRowSchema);
  }

  protected FileWriterFactory<T> newWriterFactory(Schema dataSchema) {
    return newWriterFactory(dataSchema, null, null, null);
  }

  protected abstract T toRow(Integer id, String data);

  protected PartitionKey partitionKey(PartitionSpec spec, String value) {
    Record record = GenericRecord.create(table.schema()).copy(ImmutableMap.of("data", value));

    PartitionKey partitionKey = new PartitionKey(spec, table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  protected StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  protected DataFile writeData(
      FileWriterFactory<T> writerFactory,
      OutputFileFactory fileFactory,
      List<T> rows,
      PartitionSpec spec,
      StructLike partitionKey)
      throws IOException {

    EncryptedOutputFile file = fileFactory.newOutputFile(spec, partitionKey);
    DataWriter<T> writer = writerFactory.newDataWriter(file, spec, partitionKey);

    try (DataWriter<T> closeableWriter = writer) {
      for (T row : rows) {
        closeableWriter.write(row);
      }
    }

    return writer.toDataFile();
  }
}

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

package org.apache.iceberg.flink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

public class SimpleDataUtil {

  private SimpleDataUtil() {
  }

  static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("id", DataTypes.INT())
      .field("data", DataTypes.STRING())
      .build();

  static final Record RECORD = GenericRecord.create(SCHEMA);

  static Table createTable(String path, Map<String, String> properties, boolean partitioned) {
    PartitionSpec spec;
    if (partitioned) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }
    return new HadoopTables().create(SCHEMA, spec, properties, path);
  }

  static Record createRecord(Integer id, String data) {
    Record record = RECORD.copy();
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  static RowData createRowData(Integer id, String data) {
    return GenericRowData.of(id, StringData.fromString(data));
  }

  static DataFile writeFile(Configuration conf, String location, String filename, List<RowData> rows) throws
      IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);
    switch (fileFormat) {
      case AVRO:
        FileAppender<RowData> avroAppender = Avro.write(fromPath(path, conf))
            .schema(SCHEMA)
            .createWriterFunc(ignore -> new FlinkAvroWriter(FlinkSchemaUtil.convert(SCHEMA)))
            .named(fileFormat.name())
            .build();
        try {
          avroAppender.addAll(rows);
        } finally {
          avroAppender.close();
        }

        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(HadoopInputFile.fromPath(path, conf))
            .withMetrics(avroAppender.metrics())
            .build();

      case PARQUET:
      case ORC:
        // TODO those writers once them are ready.
      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }
  }

  static void assertTableRows(String tablePath, List<RowData> rows) throws IOException {
    List<Record> records = Lists.newArrayList();
    for (RowData row : rows) {
      Integer id = row.isNullAt(0) ? null : row.getInt(0);
      String data = row.isNullAt(1) ? null : row.getString(1).toString();
      records.add(createRecord(id, data));
    }
    assertTableRecords(tablePath, records);
  }

  static void assertTableRecords(String tablePath, List<Record> expected) throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    Table newTable = new HadoopTables().load(tablePath);
    Set<Record> resultSet;
    try (CloseableIterable<Record> iterable = (CloseableIterable<Record>) IcebergGenerics.read(newTable).build()) {
      resultSet = Sets.newHashSet(iterable);
    }
    Assert.assertEquals("Should produce the expected record", resultSet, Sets.newHashSet(expected));
  }
}

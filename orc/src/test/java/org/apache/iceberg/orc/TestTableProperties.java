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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTableProperties {

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File folder;
  @TempDir private File testFile;

  @Test
  public void testOrcTablePropertiesForDataFile() throws Exception {
    String codecAsString = CompressionKind.SNAPPY.name();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            TableProperties.ORC_COMPRESSION,
            codecAsString,
            TableProperties.DEFAULT_FILE_FORMAT,
            FileFormat.ORC.name());

    String warehouse = folder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).as("Should create the table path correctly.").isTrue();

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = new HadoopTables().create(SCHEMA, spec, properties, tablePath);

    assertThat(testFile.delete()).isTrue();

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .forTable(table)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      writer.add(GenericRecord.create(SCHEMA).copy(ImmutableMap.of("id", 1, "data", "a")));
    }

    Reader reader =
        OrcFile.createReader(
            new Path(testFile.toURI()), OrcFile.readerOptions(new Configuration()));
    assertThat(reader.getCompressionKind()).isEqualTo(CompressionKind.SNAPPY);
  }

  @Test
  public void testOrcTablePropertiesForDeleteFile() throws Exception {
    String codecAsString = CompressionKind.SNAPPY.name();

    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            TableProperties.DELETE_ORC_COMPRESSION,
            codecAsString,
            TableProperties.DEFAULT_FILE_FORMAT,
            FileFormat.ORC.name());

    String warehouse = folder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).as("Should create the table path correctly.").isTrue();

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = new HadoopTables().create(SCHEMA, spec, properties, tablePath);

    assertThat(testFile.delete()).isTrue();

    try (EqualityDeleteWriter<Object> deleteWriter =
        ORC.writeDeletes(Files.localOutput(testFile))
            .forTable(table)
            .equalityFieldIds(1)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .buildEqualityWriter()) {
      deleteWriter.write(GenericRecord.create(SCHEMA).copy(ImmutableMap.of("id", 1, "data", "a")));
    }

    Reader reader =
        OrcFile.createReader(
            new Path(testFile.toURI()), OrcFile.readerOptions(new Configuration()));
    assertThat(reader.getCompressionKind()).isEqualTo(CompressionKind.SNAPPY);
  }
}

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

package org.apache.iceberg.mr.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

public class TestIcebergInputFormatHelper {

  private final HadoopTables tables;
  private final Schema schema;
  private final PartitionSpec spec;
  private final FileFormat fileFormat;
  private final TemporaryFolder tmp;
  private final File location;

  private Table table;

  public TestIcebergInputFormatHelper(HadoopTables tables, Schema schema, PartitionSpec spec, FileFormat fileFormat,
                                      TemporaryFolder tmp, File location) {
    this.tables = tables;
    this.schema = schema;
    this.spec = spec;
    this.fileFormat = fileFormat;
    this.tmp = tmp;
    this.location = location;
  }

  public Table getTable() {
    return table;
  }

  public Table createTable(Schema theSchema, PartitionSpec theSpec) {
    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    table = tables.create(theSchema, theSpec, properties, location.toString());
    return table;
  }

  public Table createTable(Catalog catalog, TableIdentifier identifier) {
    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    table = catalog.createTable(identifier, schema, spec, properties);
    return table;
  }

  public Table createPartitionedTable() {
    return createTable(schema, spec);
  }

  public Table createUnpartitionedTable() {
    return createTable(schema, PartitionSpec.unpartitioned());
  }

  public List<Record> generateRandomRecords(int num, long seed) {
    Preconditions.checkNotNull(table, "table not set");
    return RandomGenericData.generate(table.schema(), num, seed);
  }

  public void appendToTable(DataFile... dataFiles) {
    Preconditions.checkNotNull(table, "table not set");

    AppendFiles append = table.newAppend();

    for (DataFile dataFile : dataFiles) {
      append = append.appendFile(dataFile);
    }

    append.commit();
  }

  public void appendToTable(StructLike partition, List<Record> records) throws IOException {
    appendToTable(writeFile(partition, records));
  }

  public DataFile writeFile(StructLike partition, List<Record> records) throws IOException {
    Preconditions.checkNotNull(table, "table not set");

    File file = tmp.newFile();
    Assert.assertTrue(file.delete());

    FileAppender<Record> appender;

    switch (fileFormat) {
      case AVRO:
        appender = Avro.write(Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(DataWriter::create)
                .named(fileFormat.name())
                .build();
        break;

      case PARQUET:
        appender = Parquet.write(Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .named(fileFormat.name())
                .build();
        break;

      case ORC:
        appender = ORC.write(Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .build();
        break;

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }

    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFiles.Builder builder = DataFiles.builder(table.spec())
            .withPath(file.toString())
            .withFormat(fileFormat)
            .withFileSizeInBytes(file.length())
            .withMetrics(appender.metrics());

    if (partition != null) {
      builder.withPartition(partition);
    }

    return builder.build();
  }

}

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

package org.apache.iceberg.mr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
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

public class TestHelper {

  private final Configuration conf;
  private final HadoopTables tables;
  private final Schema schema;
  private final PartitionSpec spec;
  private final FileFormat fileFormat;
  private final TemporaryFolder tmp;
  private final File location;

  private Table table;

  public TestHelper(Configuration conf, HadoopTables tables, Schema schema, PartitionSpec spec, FileFormat fileFormat,
                    TemporaryFolder tmp, File location) {
    this.conf = conf;
    this.tables = tables;
    this.schema = schema;
    this.spec = spec;
    this.fileFormat = fileFormat;
    this.tmp = tmp;
    this.location = location;
  }

  private void setTable(Table table) {
    this.table = table;
    conf.set(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
  }

  public Table getTable() {
    return table;
  }

  public Table createTable(Schema theSchema, PartitionSpec theSpec) {
    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    Table tbl = tables.create(theSchema, theSpec, properties, location.toString());
    setTable(tbl);
    return tbl;
  }

  public Table createTable(Catalog catalog, TableIdentifier identifier) {
    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    Table tbl = catalog.createTable(identifier, schema, spec, properties);
    setTable(tbl);
    return tbl;
  }

  public Table createPartitionedTable() {
    return createTable(schema, spec);
  }

  public Table createUnpartitionedTable() {
    return createTable(schema, PartitionSpec.unpartitioned());
  }


  public List<Record> generateRandomRecords(int num, long seed) {
    Preconditions.checkNotNull(table, "table not set");
    return generateRandomRecords(table.schema(), num, seed);
  }

  public static List<Record> generateRandomRecords(Schema schema, int num, long seed) {
    return RandomGenericData.generate(schema, num, seed);
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
    return writeFile(table, partition, records, fileFormat, tmp.newFile());
  }

  public static DataFile writeFile(Table table, StructLike partition, List<Record> records, FileFormat fileFormat,
                                   File file) throws IOException {
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

  public static class RecordsBuilder {

    private final List<Record> records = new ArrayList<Record>();
    private final Schema schema;

    private RecordsBuilder(Schema schema) {
      this.schema = schema;
    }

    public RecordsBuilder add(Object... values) {
      Preconditions.checkArgument(schema.columns().size() == values.length);

      GenericRecord record = GenericRecord.create(schema);

      for (int i = 0; i < values.length; i++) {
        record.set(i, values[i]);
      }

      records.add(record);
      return this;
    }

    public List<Record> build() {
      return Collections.unmodifiableList(records);
    }

    public static RecordsBuilder newInstance(Schema schema) {
      return new RecordsBuilder(schema);
    }
  }
}

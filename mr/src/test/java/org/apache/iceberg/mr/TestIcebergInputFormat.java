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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;


@RunWith(Parameterized.class)
public class TestIcebergInputFormat {
  private static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(3, "id", Types.LongType.get()),
      required(2, "date", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
                                                         .identity("date")
                                                         .bucket("id", 1)
                                                         .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private HadoopTables tables;
  private Configuration conf;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{"parquet"},
        new Object[]{"avro"}
    };
  }

  private final FileFormat format;

  public TestIcebergInputFormat(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() {
    conf = new Configuration();
    tables = new HadoopTables(conf);
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    File location = temp.newFolder(format.name());
    Assert.assertTrue(location.delete());
    Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
                                location.toString());
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    DataFile dataFile = writeFile(table, null, format, expectedRecords);
    table.newAppend()
         .appendFile(dataFile)
         .commit();
    validate(conf, location.toString(), null, expectedRecords);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    File location = temp.newFolder(format.name());
    Assert.assertTrue(location.delete());
    Table table = tables.create(SCHEMA, SPEC,
                                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
                                location.toString());
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    DataFile dataFile = writeFile(table, Row.of("2020-03-20", 0), format, expectedRecords);
    table.newAppend()
         .appendFile(dataFile)
         .commit();
    validate(conf, location.toString(), null, expectedRecords);
  }

  public static class HadoopCatalogFunc implements Function<Configuration, Catalog> {
    @Override
    public Catalog apply(Configuration conf) {
      return new HadoopCatalog(conf, conf.get("warehouse.location"));
    }
  }

  @Test
  public void testCustomCatalog() throws Exception {
    conf = new Configuration();
    conf.set("warehouse.location", temp.newFolder("hadoop_catalog").getAbsolutePath());

    Catalog catalog = new HadoopCatalogFunc().apply(conf);
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "t");
    Table table = catalog.createTable(tableIdentifier, SCHEMA, SPEC,
                                      ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    DataFile dataFile = writeFile(table, Row.of("2020-03-20", 0), format, expectedRecords);
    table.newAppend()
         .appendFile(dataFile)
         .commit();
    validate(conf, tableIdentifier.toString(), HadoopCatalogFunc.class, expectedRecords);
  }

  private static void validate(
      Configuration conf, String path, Class<? extends Function<Configuration, Catalog>> catalogFuncClass,
      List<Record> expectedRecords) throws IOException {
    Job job = Job.getInstance(conf);
    IcebergInputFormat.ConfigBuilder configBuilder = IcebergInputFormat.configure(job);
    if (catalogFuncClass != null) {
      configBuilder.catalogFunc(catalogFuncClass);
    }
    configBuilder.readFrom(path);
    List<Record> actualRecords = readRecords(job.getConfiguration());
    Assert.assertEquals(expectedRecords, actualRecords);
  }

  private static <T> List<T> readRecords(Configuration conf) {
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    IcebergInputFormat<T> icebergInputFormat = new IcebergInputFormat<>();
    List<InputSplit> splits = icebergInputFormat.getSplits(context);
    return
        FluentIterable
            .from(splits)
            .transformAndConcat(split -> readRecords(icebergInputFormat, split, context))
            .toList();
  }

  private static <T> Iterable<T> readRecords(
      IcebergInputFormat<T> inputFormat, InputSplit split, TaskAttemptContext context) {
    RecordReader<Void, T> recordReader = inputFormat.createRecordReader(split, context);
    List<T> records = new ArrayList<>();
    try {
      recordReader.initialize(split, context);
      while (recordReader.nextKeyValue()) {
        records.add(recordReader.getCurrentValue());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return records;
  }

  private DataFile writeFile(
      Table table, StructLike partitionData, FileFormat fileFormat, List<Record> records) throws IOException {
    File file = temp.newFile();
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
      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }

    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    final DataFiles.Builder builder = DataFiles.builder(table.spec())
                                               .withPath(file.toString())
                                               .withFormat(format)
                                               .withFileSizeInBytes(file.length())
                                               .withMetrics(appender.metrics());
    if (partitionData != null) {
      builder.withPartition(partitionData);
    }
    return builder.build();
  }
}

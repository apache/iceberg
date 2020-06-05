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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
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
  static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()),
      required(3, "date", Types.StringType.get()));

  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("date")
      .bucket("id", 1)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  // before variables
  private HadoopTables tables;
  private Configuration conf;
  private File location;
  private TestIcebergInputFormatHelper helper;

  // parametrized variables
  private final FileFormat format;

  @Before
  public void before() throws IOException {
    conf = new Configuration();
    tables = new HadoopTables(conf);

    location = temp.newFolder(format.name());
    Assert.assertTrue(location.delete());

    helper = new TestIcebergInputFormatHelper(tables, SCHEMA, SPEC, format, temp, location);
  }

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{"parquet"},
        new Object[]{"avro"},
        new Object[]{"orc"}
    };
  }

  public TestIcebergInputFormat(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    helper.createUnpartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);

    Job job = Job.getInstance(conf);
    IcebergInputFormat.configure(job).readFrom(location);
    validate(job, expectedRecords);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    helper.createPartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    Job job = Job.getInstance(conf);
    IcebergInputFormat.configure(job).readFrom(location);
    validate(job, expectedRecords);
  }

  @Test
  public void testFilterExp() throws Exception {
    helper.createPartitionedTable();

    List<Record> expectedRecords = helper.generateRandomRecords(2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    DataFile dataFile1 = helper.writeFile(Row.of("2020-03-20", 0), expectedRecords);
    DataFile dataFile2 = helper.writeFile(Row.of("2020-03-21", 0), helper.generateRandomRecords(2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    Job job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .readFrom(location)
            .filter(Expressions.equal("date", "2020-03-20"));

    validate(job, expectedRecords);
  }

  @Test
  public void testResiduals() throws Exception {
    helper.createPartitionedTable();

    List<Record> writeRecords = helper.generateRandomRecords(2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    List<Record> expectedRecords = new ArrayList<>();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 = helper.writeFile(Row.of("2020-03-21", 0), helper.generateRandomRecords(2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    Job job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .readFrom(location)
            .filter(Expressions.and(
                    Expressions.equal("date", "2020-03-20"),
                    Expressions.equal("id", 123)));

    validate(job, expectedRecords);

    // skip residual filtering
    job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .readFrom(location)
            .filter(Expressions.and(
                    Expressions.equal("date", "2020-03-20"),
                    Expressions.equal("id", 123)))
            .skipResidualFiltering();

    validate(job, writeRecords);
  }

  @Test
  public void testFailedResidualFiltering() throws Exception {
    helper.createPartitionedTable();

    List<Record> expectedRecords = helper.generateRandomRecords(2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    Job jobShouldFail1 = Job.getInstance(conf);
    IcebergInputFormat.configure(jobShouldFail1)
            .readFrom(location)
            .useHiveRows()
            .filter(Expressions.and(
                    Expressions.equal("date", "2020-03-20"),
                    Expressions.equal("id", 0)));

    AssertHelpers.assertThrows(
        "Residuals are not evaluated today for Iceberg Generics In memory model of HIVE",
        UnsupportedOperationException.class, "Filter expression ref(name=\"id\") == 0 is not completely satisfied.",
        () -> validate(jobShouldFail1, expectedRecords));

    Job jobShouldFail2 = Job.getInstance(conf);
    IcebergInputFormat.configure(jobShouldFail2)
            .readFrom(location)
            .usePigTuples()
            .filter(Expressions.and(
                    Expressions.equal("date", "2020-03-20"),
                    Expressions.equal("id", 0)));

    AssertHelpers.assertThrows(
        "Residuals are not evaluated today for Iceberg Generics In memory model of PIG",
        UnsupportedOperationException.class, "Filter expression ref(name=\"id\") == 0 is not completely satisfied.",
        () -> validate(jobShouldFail2, expectedRecords));
  }

  @Test
  public void testProjection() throws Exception {
    helper.createPartitionedTable();
    List<Record> inputRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(Row.of("2020-03-20", 0), inputRecords);

    Schema projection = TypeUtil.select(SCHEMA, ImmutableSet.of(1));

    Job job = Job.getInstance(conf);
    IcebergInputFormat.configure(job)
            .readFrom(location)
            .project(projection);

    List<Record> outputRecords = readRecords(job.getConfiguration());

    Assert.assertEquals(inputRecords.size(), outputRecords.size());
    Assert.assertEquals(projection.asStruct(), outputRecords.get(0).struct());
  }

  private static final Schema LOG_SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "date", Types.StringType.get()),
      Types.NestedField.optional(3, "level", Types.StringType.get()),
      Types.NestedField.optional(4, "message", Types.StringType.get())
  );

  private static final PartitionSpec IDENTITY_PARTITION_SPEC =
      PartitionSpec.builderFor(LOG_SCHEMA).identity("date").identity("level").build();

  @Test
  public void testIdentityPartitionProjections() throws Exception {
    helper.createTable(LOG_SCHEMA, IDENTITY_PARTITION_SPEC);
    List<Record> inputRecords = helper.generateRandomRecords(10, 0L);

    Integer idx = 0;
    AppendFiles append = helper.getTable().newAppend();
    for (Record record : inputRecords) {
      record.set(1, "2020-03-2" + idx);
      record.set(2, idx.toString());
      append.appendFile(helper.writeFile(Row.of("2020-03-2" + idx, idx.toString()), ImmutableList.of(record)));
      idx += 1;
    }
    append.commit();

    // individual fields
    validateIdentityPartitionProjections(withColumns("date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("id"), inputRecords);
    // field pairs
    validateIdentityPartitionProjections(withColumns("date", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("date", "level"), inputRecords);
    // out-of-order pairs
    validateIdentityPartitionProjections(withColumns("message", "date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "date"), inputRecords);
    // full projection
    validateIdentityPartitionProjections(LOG_SCHEMA, inputRecords);
    // out-of-order triplets
    validateIdentityPartitionProjections(withColumns("date", "level", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "date", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("date", "message", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "message", "date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "date", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "level", "date"), inputRecords);
  }

  private static Schema withColumns(String... names) {
    Map<String, Integer> indexByName = TypeUtil.indexByName(LOG_SCHEMA.asStruct());
    Set<Integer> projectedIds = Sets.newHashSet();
    for (String name : names) {
      projectedIds.add(indexByName.get(name));
    }
    return TypeUtil.select(LOG_SCHEMA, projectedIds);
  }

  private void validateIdentityPartitionProjections(Schema projectedSchema, List<Record> inputRecords) throws Exception {
    Job job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .readFrom(location)
            .project(projectedSchema);

    List<Record> actualRecords = readRecords(job.getConfiguration());

    Set<String> fieldNames = TypeUtil.indexByName(projectedSchema.asStruct()).keySet();
    for (int pos = 0; pos < inputRecords.size(); pos++) {
      Record inputRecord = inputRecords.get(pos);
      Record actualRecord = actualRecords.get(pos);
      Assert.assertEquals("Projected schema should match", projectedSchema.asStruct(), actualRecord.struct());
      for (String name : fieldNames) {
        Assert.assertEquals(
            "Projected field " + name + " should match", inputRecord.getField(name), actualRecord.getField(name));
      }
    }
  }

  @Test
  public void testSnapshotReads() throws Exception {
    helper.createUnpartitionedTable();

    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);
    long snapshotId = helper.getTable().currentSnapshot().snapshotId();

    helper.appendToTable(null, helper.generateRandomRecords(1, 0L));

    Job job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .readFrom(location)
            .snapshotId(snapshotId);

    validate(job, expectedRecords);
  }

  @Test
  public void testLocality() throws Exception {
    helper.createUnpartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);

    Job job = Job.getInstance(conf);
    IcebergMRConfig.Builder builder = IcebergInputFormat.configure(job).readFrom(location);

    for (InputSplit split : splits(job.getConfiguration())) {
      Assert.assertArrayEquals(IcebergSplit.ANYWHERE, split.getLocations());
    }

    builder.preferLocality();

    for (InputSplit split : splits(job.getConfiguration())) {
      Assert.assertArrayEquals(new String[]{"localhost"}, split.getLocations());
    }
  }

  public static class HadoopCatalogLoader implements Function<Configuration, Catalog> {
    @Override
    public Catalog apply(Configuration conf) {
      return new HadoopCatalog(conf, conf.get("warehouse.location"));
    }
  }

  @Test
  public void testCustomCatalog() throws IOException {
    conf.set("warehouse.location", temp.newFolder("hadoop_catalog").getAbsolutePath());

    Catalog catalog = new HadoopCatalogLoader().apply(conf);
    TableIdentifier identifier = TableIdentifier.of("db", "t");
    helper.createTable(catalog, identifier);

    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    Job job = Job.getInstance(conf);

    IcebergInputFormat.configure(job)
            .catalogLoader(HadoopCatalogLoader.class)
            .readFrom(identifier);

    validate(job, expectedRecords);
  }

  private static void validate(Job job, List<Record> expectedRecords) {
    List<Record> actualRecords = readRecords(job.getConfiguration());
    Assert.assertEquals(expectedRecords, actualRecords);
  }

  private static <T> List<InputSplit> splits(Configuration conf) {
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    IcebergInputFormat<T> icebergInputFormat = new IcebergInputFormat<>();
    return icebergInputFormat.getSplits(context);
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

}

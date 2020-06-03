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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.DataFiles.fromInputFile;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterables.concat;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterables.filter;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterables.transform;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestLocalScan {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));

  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "orc" },
        new Object[] { "avro" }
    };
  }

  private final FileFormat format;

  public TestLocalScan(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  private String sharedTableLocation = null;
  private Table sharedTable = null;
  private List<Record> file1Records = null;
  private List<Record> file2Records = null;
  private List<Record> file3Records = null;
  private List<Record> file1FirstSnapshotRecords = null;
  private List<Record> file2FirstSnapshotRecords = null;
  private List<Record> file3FirstSnapshotRecords = null;
  private List<Record> file1SecondSnapshotRecords = null;
  private List<Record> file2SecondSnapshotRecords = null;
  private List<Record> file3SecondSnapshotRecords = null;


  private void overwriteExistingData() throws IOException {
    Record record = GenericRecord.create(SCHEMA);

    this.file1FirstSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 4L, "data", "obscure")),
        record.copy(ImmutableMap.of("id", 5L, "data", "secure")),
        record.copy(ImmutableMap.of("id", 6L, "data", "fetta"))
    );
    DataFile file11 = writeFile(sharedTableLocation, format.addExtension("file-11"), file1FirstSnapshotRecords);

    this.file2FirstSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 14L, "data", "radical")),
        record.copy(ImmutableMap.of("id", 15L, "data", "collocation")),
        record.copy(ImmutableMap.of("id", 16L, "data", "book"))
    );
    DataFile file21 = writeFile(sharedTableLocation, format.addExtension("file-21"), file2FirstSnapshotRecords);

    this.file3FirstSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 24L, "data", "cloud")),
        record.copy(ImmutableMap.of("id", 25L, "data", "zen")),
        record.copy(ImmutableMap.of("id", 26L, "data", "sky"))
    );
    DataFile file31 = writeFile(sharedTableLocation, format.addExtension("file-31"), file3FirstSnapshotRecords);

    sharedTable.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(file11)
        .addFile(file21)
        .addFile(file31)
        .commit();

    this.file1SecondSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 6L, "data", "brainy")),
        record.copy(ImmutableMap.of("id", 7L, "data", "film")),
        record.copy(ImmutableMap.of("id", 8L, "data", "fetta"))
    );
    DataFile file12 = writeFile(sharedTableLocation, format.addExtension("file-12"), file1SecondSnapshotRecords);

    this.file2SecondSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 16L, "data", "cake")),
        record.copy(ImmutableMap.of("id", 17L, "data", "intrinsic")),
        record.copy(ImmutableMap.of("id", 18L, "data", "paper"))
    );
    DataFile file22 = writeFile(sharedTableLocation, format.addExtension("file-22"), file2SecondSnapshotRecords);

    this.file3SecondSnapshotRecords = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 26L, "data", "belleview")),
        record.copy(ImmutableMap.of("id", 27L, "data", "overview")),
        record.copy(ImmutableMap.of("id", 28L, "data", "tender"))
    );
    DataFile file32 = writeFile(sharedTableLocation, format.addExtension("file-32"), file3SecondSnapshotRecords);

    sharedTable.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(file12)
        .addFile(file22)
        .addFile(file32)
        .commit();
  }

  @Before
  public void createTables() throws IOException {
    File location = temp.newFolder("shared");
    Assert.assertTrue(location.delete());
    this.sharedTableLocation = location.toString();
    this.sharedTable = TABLES.create(
        SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
        sharedTableLocation);

    Record record = GenericRecord.create(SCHEMA);

    this.file1Records = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 0L, "data", "clarification")),
        record.copy(ImmutableMap.of("id", 1L, "data", "risky")),
        record.copy(ImmutableMap.of("id", 2L, "data", "falafel"))
    );
    DataFile file1 = writeFile(sharedTableLocation, format.addExtension("file-1"), file1Records);

    Record nullData = record.copy();
    nullData.setField("id", 11L);
    nullData.setField("data", null);

    this.file2Records = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 10L, "data", "clammy")),
        record.copy(ImmutableMap.of("id", 11L, "data", "evacuate")),
        record.copy(ImmutableMap.of("id", 12L, "data", "tissue"))
    );
    DataFile file2 = writeFile(sharedTableLocation, format.addExtension("file-2"), file2Records);

    this.file3Records = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 20L, "data", "ocean")),
        record.copy(ImmutableMap.of("id", 21L, "data", "holistic")),
        record.copy(ImmutableMap.of("id", 22L, "data", "preventative"))
    );
    DataFile file3 = writeFile(sharedTableLocation, format.addExtension("file-3"), file3Records);

    // commit the test data
    sharedTable.newAppend()
        .appendFile(file1)
        .appendFile(file2)
        .appendFile(file3)
        .commit();
  }

  @Test
  public void testRandomData() throws IOException {
    List<Record> expected = RandomGenericData.generate(SCHEMA, 1000, 435691832918L);

    File location = temp.newFolder(format.name());
    Assert.assertTrue(location.delete());
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
        location.toString());

    AppendFiles append = table.newAppend();

    int fileNum = 0;
    int recordsPerFile = 200;
    Iterator<Record> iter = expected.iterator();
    while (iter.hasNext()) {
      Path path = new Path(location.toString(), format.addExtension("file-" + fileNum));
      int numRecords;

      List<Record> records = Lists.newArrayList();
      for (numRecords = 0; numRecords < recordsPerFile && iter.hasNext(); numRecords += 1) {
        records.add(iter.next());
      }

      writeFile(location.toString(), format.addExtension("file-" + fileNum), records);
      append.appendFile(fromInputFile(HadoopInputFile.fromPath(path, CONF), numRecords));

      fileNum += 1;
    }

    append.commit();

    Set<Record> records = Sets.newHashSet(IcebergGenerics.read(table).build());
    Assert.assertEquals("Should produce correct number of records",
        expected.size(), records.size());
    Assert.assertEquals("Random record set should match",
        Sets.newHashSet(expected), records);
  }

  @Test
  public void testFullScan() {
    Iterable<Record> results = IcebergGenerics.read(sharedTable).build();

    Set<Record> expected = Sets.newHashSet();
    expected.addAll(file1Records);
    expected.addAll(file2Records);
    expected.addAll(file3Records);

    Set<Record> records = Sets.newHashSet(results);
    Assert.assertEquals("Should produce correct number of records",
        expected.size(), records.size());
    Assert.assertEquals("Random record set should match",
        Sets.newHashSet(expected), records);
  }

  @Test
  public void testFilter() {
    Iterable<Record> result = IcebergGenerics.read(sharedTable).where(lessThan("id", 3)).build();

    Assert.assertEquals("Records should match file 1",
        Sets.newHashSet(file1Records), Sets.newHashSet(result));

    result = IcebergGenerics.read(sharedTable).where(lessThanOrEqual("id", 1)).build();

    Assert.assertEquals("Records should match file 1 without id 2",
        Sets.newHashSet(filter(file1Records, r -> (Long) r.getField("id") <= 1)),
        Sets.newHashSet(result));
  }

  @Test
  public void testProject() {
    Iterable<Record> results = IcebergGenerics.read(sharedTable).select("id").build();

    Set<Long> expected = Sets.newHashSet();
    expected.addAll(Lists.transform(file1Records, record -> (Long) record.getField("id")));
    expected.addAll(Lists.transform(file2Records, record -> (Long) record.getField("id")));
    expected.addAll(Lists.transform(file3Records, record -> (Long) record.getField("id")));

    results.forEach(record ->
        Assert.assertEquals("Record should have one projected field", 1, record.size()));

    Assert.assertEquals("Should project only id columns",
        expected, Sets.newHashSet(transform(results, record -> (Long) record.getField("id"))));
  }

  @Test
  public void testProjectWithMissingFilterColumn() {
    Iterable<Record> results = IcebergGenerics.read(sharedTable)
        .where(Expressions.greaterThanOrEqual("id", 1))
        .where(Expressions.lessThan("id", 21))
        .select("data").build();

    Set<String> expected = Sets.newHashSet();
    for (Record record : concat(file1Records, file2Records, file3Records)) {
      Long id = (Long) record.getField("id");
      if (id >= 1 && id < 21) {
        expected.add(record.getField("data").toString());
      }
    }

    results.forEach(record ->
        Assert.assertEquals("Record should have two projected fields", 2, record.size()));

    Assert.assertEquals("Should project correct rows",
        expected,
        Sets.newHashSet(transform(results, record -> record.getField("data").toString())));
  }

  @Test
  public void testUseSnapshot() throws IOException {
    overwriteExistingData();
    Iterable<Record> results = IcebergGenerics.read(sharedTable)
        .useSnapshot(/* first snapshot */ sharedTable.history().get(1).snapshotId())
        .build();

    Set<Record> expected = Sets.newHashSet();
    expected.addAll(file1FirstSnapshotRecords);
    expected.addAll(file2FirstSnapshotRecords);
    expected.addAll(file3FirstSnapshotRecords);

    Set<Record> records = Sets.newHashSet(results);
    Assert.assertEquals("Should produce correct number of records",
        expected.size(), records.size());
    Assert.assertEquals("Record set should match",
        Sets.newHashSet(expected), records);
    Assert.assertNotNull(Iterables.get(records, 0).getField("id"));
    Assert.assertNotNull(Iterables.get(records, 0).getField("data"));
  }

  @Test
  public void testAsOfTime() throws IOException {
    overwriteExistingData();
    Iterable<Record> results = IcebergGenerics.read(sharedTable)
        .asOfTime(/* timestamp first snapshot */ sharedTable.history().get(2).timestampMillis())
        .build();

    Set<Record> expected = Sets.newHashSet();
    expected.addAll(file1SecondSnapshotRecords);
    expected.addAll(file2SecondSnapshotRecords);
    expected.addAll(file3SecondSnapshotRecords);

    Set<Record> records = Sets.newHashSet(results);
    Assert.assertEquals("Should produce correct number of records",
        expected.size(), records.size());
    Assert.assertEquals("Record set should match",
        Sets.newHashSet(expected), records);
    Assert.assertNotNull(Iterables.get(records, 0).getField("id"));
    Assert.assertNotNull(Iterables.get(records, 0).getField("data"));
  }

  @Test
  public void testUnknownSnapshotId() {
    Long minSnapshotId = sharedTable.history().stream().map(h -> h.snapshotId()).min(Long::compareTo).get();

    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(sharedTable);

    AssertHelpers.assertThrows("Should fail on unknown snapshot id",
        IllegalArgumentException.class,
        "Cannot find snapshot with ID ",
        () -> scanBuilder.useSnapshot(/* unknown snapshot id */ minSnapshotId - 1));
  }

  @Test
  public void testAsOfTimeOlderThanFirstSnapshot() {
    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(sharedTable);

    AssertHelpers.assertThrows("Should fail on timestamp sooner than first write",
        IllegalArgumentException.class,
        "Cannot find a snapshot older than ",
        () -> scanBuilder.asOfTime(/* older than first snapshot */ sharedTable.history().get(0).timestampMillis() - 1));
  }

  private DataFile writeFile(String location, String filename, List<Record> records) throws IOException {
    return writeFile(location, filename, SCHEMA, records);
  }

  private DataFile writeFile(String location, String filename, Schema schema, List<Record> records) throws IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);
    switch (fileFormat) {
      case AVRO:
        FileAppender<Record> avroAppender = Avro.write(fromPath(path, CONF))
            .schema(schema)
            .createWriterFunc(DataWriter::create)
            .named(fileFormat.name())
            .build();
        try {
          avroAppender.addAll(records);
        } finally {
          avroAppender.close();
        }

        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(HadoopInputFile.fromPath(path, CONF))
            .withMetrics(avroAppender.metrics())
            .build();

      case PARQUET:
        FileAppender<Record> parquetAppender = Parquet.write(fromPath(path, CONF))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();
        try {
          parquetAppender.addAll(records);
        } finally {
          parquetAppender.close();
        }

        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(HadoopInputFile.fromPath(path, CONF))
            .withMetrics(parquetAppender.metrics())
            .build();

      case ORC:
        FileAppender<Record> orcAppender = ORC.write(fromPath(path, CONF))
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build();
        try {
          orcAppender.addAll(records);
        } finally {
          orcAppender.close();
        }

        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(HadoopInputFile.fromPath(path, CONF))
                .withMetrics(orcAppender.metrics())
                .build();

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }
  }

  @Test
  public void testFilterWithDateAndTimestamp() throws IOException {
    Schema schema = new Schema(
        required(1, "timestamp_with_zone", Types.TimestampType.withZone()),
        required(2, "timestamp_without_zone", Types.TimestampType.withoutZone()),
        required(3, "date", Types.DateType.get()),
        required(4, "time", Types.TimeType.get())
    );

    File tableLocation = temp.newFolder("complex_filter_table");
    Assert.assertTrue(tableLocation.delete());

    Table table = TABLES.create(
        schema, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()),
        tableLocation.getAbsolutePath());

    List<Record> expected = RandomGenericData.generate(schema, 100, 435691832918L);
    DataFile file = writeFile(tableLocation.toString(), format.addExtension("record-file"), schema, expected);
    table.newFastAppend().appendFile(file).commit();

    for (Record r : expected) {
      Iterable<Record> filterResult = IcebergGenerics.read(table)
          .where(equal("timestamp_with_zone", r.getField("timestamp_with_zone").toString()))
          .where(equal("timestamp_without_zone", r.getField("timestamp_without_zone").toString()))
          .where(equal("date", r.getField("date").toString()))
          .where(equal("time", r.getField("time").toString()))
          .build();

      Assert.assertTrue(filterResult.iterator().hasNext());
      Record readRecord = filterResult.iterator().next();
      Assert.assertEquals(r.getField("timestamp_with_zone"), readRecord.getField("timestamp_with_zone"));
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}

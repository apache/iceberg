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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.iceberg.DataFiles.fromInputFile;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
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
    InputFile file1 = writeFile(sharedTableLocation, format.addExtension("file-1"), file1Records);

    Record nullData = record.copy();
    nullData.setField("id", 11L);
    nullData.setField("data", null);

    this.file2Records = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 10L, "data", "clammy")),
        record.copy(ImmutableMap.of("id", 11L, "data", "evacuate")),
        record.copy(ImmutableMap.of("id", 12L, "data", "tissue"))
    );
    InputFile file2 = writeFile(sharedTableLocation, format.addExtension("file-2"), file2Records);

    this.file3Records = Lists.newArrayList(
        record.copy(ImmutableMap.of("id", 20L, "data", "ocean")),
        record.copy(ImmutableMap.of("id", 21L, "data", "holistic")),
        record.copy(ImmutableMap.of("id", 22L, "data", "preventative"))
    );
    InputFile file3 = writeFile(sharedTableLocation, format.addExtension("file-3"), file3Records);

    // commit the test data
    sharedTable.newAppend()
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(file1)
            .withMetrics(new Metrics(3L,
                null, // no column sizes
                ImmutableMap.of(1, 3L), // value count
                ImmutableMap.of(1, 0L), // null count
                ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                ImmutableMap.of(1, longToBuffer(2L)))) // upper bounds)
            .build())
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(file2)
            .withMetrics(new Metrics(3L,
                null, // no column sizes
                ImmutableMap.of(1, 3L), // value count
                ImmutableMap.of(1, 0L), // null count
                ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
                ImmutableMap.of(1, longToBuffer(12L)))) // upper bounds)
            .build())
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(file3)
            .withMetrics(new Metrics(3L,
                null, // no column sizes
                ImmutableMap.of(1, 3L), // value count
                ImmutableMap.of(1, 0L), // null count
                ImmutableMap.of(1, longToBuffer(20L)), // lower bounds
                ImmutableMap.of(1, longToBuffer(22L)))) // upper bounds)
            .build())
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

  private InputFile writeFile(String location, String filename, List<Record> records) throws IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);
    switch (fileFormat) {
      case AVRO:
        try (FileAppender<Record> appender = Avro.write(fromPath(path, CONF))
            .schema(SCHEMA)
            .createWriterFunc(DataWriter::create)
            .named(fileFormat.name())
            .build()) {
          appender.addAll(records);
        }

        return HadoopInputFile.fromPath(path, CONF);

      case PARQUET:
        try (FileAppender<Record> appender = Parquet.write(fromPath(path, CONF))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
          appender.addAll(records);
        }

        return HadoopInputFile.fromPath(path, CONF);

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}

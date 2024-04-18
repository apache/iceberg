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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.TableProperties.PARQUET_DICT_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroWriter;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkParquetPageSkipping {

  private static final Types.StructType PRIMITIVES =
      Types.StructType.of(
          required(0, "_long", Types.LongType.get()),
          optional(1, "_string", Types.StringType.get()), // var width
          required(2, "_bool", Types.BooleanType.get()),
          optional(3, "_int", Types.IntegerType.get()),
          optional(4, "_float", Types.FloatType.get()),
          required(5, "_double", Types.DoubleType.get()),
          optional(6, "_date", Types.DateType.get()),
          required(7, "_ts", Types.TimestampType.withZone()),
          required(8, "_fixed", Types.FixedType.ofLength(7)),
          optional(9, "_bytes", Types.BinaryType.get()), // var width
          required(10, "_dec_9_0", Types.DecimalType.of(9, 0)), // int
          required(11, "_dec_11_2", Types.DecimalType.of(11, 2)), // long
          required(12, "_dec_38_10", Types.DecimalType.of(38, 10)) // fixed
          );

  private static final Schema PRIMITIVES_SCHEMA = new Schema(PRIMITIVES.fields());

  private static final Types.StructType LIST =
      Types.StructType.of(
          optional(13, "_list", Types.ListType.ofOptional(14, Types.StringType.get())));
  private static final Types.StructType MAP =
      Types.StructType.of(
          optional(
              15,
              "_map",
              Types.MapType.ofOptional(16, 17, Types.StringType.get(), Types.StringType.get())));
  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          Lists.newArrayList(Iterables.concat(PRIMITIVES.fields(), LIST.fields(), MAP.fields())));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private File testFile;
  private List<GenericData.Record> allRecords = Lists.newArrayList();
  private List<GenericData.Record> rowGroup0;

  /* Column and offset indexes info of `_long` column in `testFile` copied from text printed by parquet-cli's
  column-index command:

  row-group 0:
  column index for column _long:
  Boudary order: ASCENDING
                      null count  min                                       max
  page-0                         0  0                                         56
  page-1                         0  57                                        113
  page-2                         0  114                                       170
  page-3                         0  171                                       227
  page-4                         0  228                                       284
  page-5                         0  285                                       341
  page-6                         0  342                                       398
  page-7                         0  399                                       455
  page-8                         0  456                                       512
  page-9                         0  513                                       569
  page-10                        0  570                                       592

  offset index for column _long:
  offset   compressed size       first row index
  page-0                         4               137                     0
  page-1                       141               138                    57
  page-2                       279               137                   114
  page-3                       416               138                   171
  page-4                       554               137                   228
  page-5                       691               141                   285
  page-6                       832               140                   342
  page-7                       972               141                   399
  page-8                      1113               141                   456
  page-9                      1254               140                   513
  page-10                     1394                92                   570


  row-group 1:
  column index for column _long:
  Boudary order: ASCENDING
                      null count  min                                       max
  page-0                         0  593                                       649
  page-1                         0  650                                       706
  page-2                         0  707                                       763
  page-3                         0  764                                       820
  page-4                         0  821                                       877
  page-5                         0  878                                       934
  page-6                         0  935                                       991
  page-7                         0  992                                       999

  offset index for column _long:
  offset   compressed size       first row index
  page-0                    498681               140                     0
  page-1                    498821               140                    57
  page-2                    498961               141                   114
  page-3                    499102               141                   171
  page-4                    499243               141                   228
  page-5                    499384               140                   285
  page-6                    499524               142                   342
  page-7                    499666                68                   399
  */

  private long index = -1;
  private static final int ABOVE_INT_COL_MAX_VALUE = Integer.MAX_VALUE;

  @Before
  public void generateFile() throws IOException {
    testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    Function<GenericData.Record, GenericData.Record> transform =
        record -> {
          index += 1;
          if (record.get("_long") != null) {
            record.put("_long", index);
          }

          if (Objects.equals(record.get("_int"), ABOVE_INT_COL_MAX_VALUE)) {
            record.put("_int", ABOVE_INT_COL_MAX_VALUE - 1);
          }

          return record;
        };

    int numRecords = 1000;
    allRecords =
        RandomData.generateList(COMPLEX_SCHEMA, numRecords, 0).stream()
            .map(transform)
            .collect(Collectors.toList());
    rowGroup0 = selectRecords(allRecords, Pair.of(0, 593));

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(testFile))
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .schema(COMPLEX_SCHEMA)
            .set(PARQUET_PAGE_SIZE_BYTES, "500")
            .set(PARQUET_ROW_GROUP_SIZE_BYTES, "500000") // 2 row groups
            .set(PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
            .set(PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
            .set(PARQUET_DICT_SIZE_BYTES, "1")
            .named("pages_unaligned_file")
            .build()) {
      writer.addAll(allRecords);
    }
  }

  @Parameterized.Parameters(name = "vectorized = {0}")
  public static Object[] parameters() {
    return new Object[] {false, true};
  }

  private final boolean vectorized;

  public TestSparkParquetPageSkipping(boolean vectorized) {
    this.vectorized = vectorized;
  }

  @Test
  public void testSinglePageMatch() {
    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("_long", 57),
            Expressions.lessThan("_long", 114)); // exactly page-1  -> row ranges: [57, 113]

    List<GenericData.Record> expected = selectRecords(allRecords, Pair.of(57, 114));
    readAndValidate(filter, expected, rowGroup0);
  }

  @Test
  public void testMultiplePagesMatch() {
    Expression filter =
        Expressions.or(
            // page-1  -> row ranges: [57, 113]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 57), Expressions.lessThan("_long", 114)),

            // page-3, page-4 in row group 0  -> row ranges[171, 284]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 173), Expressions.lessThan("_long", 260)));

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(171, 285));
    readAndValidate(filter, expected, rowGroup0);
  }

  @Test
  public void testMultipleRowGroupsMatch() {
    Expression filter =
        Expressions.or(
            // page-1  -> row ranges: [57, 113]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 57), Expressions.lessThan("_long", 114)),

            // page-3, page-4 in row group 0  -> row ranges[171, 284]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 173), Expressions.lessThan("_long", 260)));

    filter =
        Expressions.or(
            filter,
            // page-10 in row group 0 and page-0, page-1 in row group 1 -> row ranges: [570, 706]
            Expressions.and(
                Expressions.greaterThanOrEqual("_long", 572), Expressions.lessThan("_long", 663)));

    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(171, 285), Pair.of(570, 707));
    readAndValidate(filter, expected, allRecords);
  }

  @Test
  public void testOnlyFilterPagesOnOneRowGroup() {
    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("_long", 57),
            Expressions.lessThan("_long", 114)); // exactly page-1  -> row ranges: [57, 113]

    filter =
        Expressions.or(
            filter,
            // page-9, page-10 in row group 0 -> row ranges: [513, 592]
            // and all pages in row group 1
            Expressions.greaterThanOrEqual("_long", 569));

    // some pages of row group 0 and all pages of row group 1
    List<GenericData.Record> expected =
        selectRecords(allRecords, Pair.of(57, 114), Pair.of(513, 593), Pair.of(593, 1000));

    readAndValidate(filter, expected, allRecords);
  }

  @Test
  public void testNoRowsMatch() {
    Expression filter =
        Expressions.and(
            Expressions.and(
                Expressions.greaterThan("_long", 40), Expressions.lessThan("_long", 46)),
            Expressions.equal("_int", ABOVE_INT_COL_MAX_VALUE));

    readAndValidate(filter, ImmutableList.of(), ImmutableList.of());
  }

  @Test
  public void testAllRowsMatch() {
    Expression filter = Expressions.greaterThanOrEqual("_long", Long.MIN_VALUE);
    readAndValidate(filter, allRecords, allRecords);
  }

  private Schema readSchema() {
    return vectorized ? PRIMITIVES_SCHEMA : COMPLEX_SCHEMA;
  }

  private void readAndValidate(
      Expression filter,
      List<GenericData.Record> expected,
      List<GenericData.Record> vectorizedExpected) {
    Schema projected = readSchema();

    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(testFile)).project(projected).filter(filter);

    Types.StructType struct = projected.asStruct();

    if (vectorized) {
      CloseableIterable<ColumnarBatch> batches =
          builder
              .createBatchedReaderFunc(
                  type ->
                      VectorizedSparkParquetReaders.buildReader(
                          projected, type, ImmutableMap.of(), null))
              .build();

      Iterator<GenericData.Record> expectedIterator = vectorizedExpected.iterator();
      for (ColumnarBatch batch : batches) {
        TestHelpers.assertEqualsBatch(struct, expectedIterator, batch);
      }

      Assert.assertFalse(
          "The expected records is more than the actual result", expectedIterator.hasNext());
    } else {
      CloseableIterable<InternalRow> reader =
          builder
              .createReaderFunc(type -> SparkParquetReaders.buildReader(projected, type))
              .build();
      CloseableIterator<InternalRow> actualRows = reader.iterator();

      for (GenericData.Record record : expected) {
        Assert.assertTrue("Should have expected number of rows", actualRows.hasNext());
        InternalRow row = actualRows.next();
        TestHelpers.assertEqualsUnsafe(struct, record, row);
      }

      Assert.assertFalse("Should not have extra rows", actualRows.hasNext());
    }
  }

  private List<GenericData.Record> selectRecords(
      List<GenericData.Record> records, Pair<Integer, Integer>... ranges) {
    return Arrays.stream(ranges)
        .map(range -> records.subList(range.first(), range.second()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}

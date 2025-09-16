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
package org.apache.iceberg.data.parquet;

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.MetricsRowGroupFilterTestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

public class TestParquetMetricsRowGroupFilter extends MetricsRowGroupFilterTestBase {

  private static final Schema PARQUET_SCHEMA =
      new Schema(
          Lists.newArrayList(
              Iterables.concat(
                  BASE_SCHEMA.columns(),
                  Lists.newArrayList(
                      optional(17, "no_stats_parquet", StringType.get()),
                      optional(18, "some_nulls_variant", Types.VariantType.get()),
                      optional(19, "all_nulls_variant", Types.VariantType.get())))));

  private static final Schema PARQUET_FILE_SCHEMA =
      new Schema(
          Lists.newArrayList(
              Iterables.concat(
                  BASE_FILE_SCHEMA.columns(),
                  Lists.newArrayList(
                      optional(17, "_no_stats_parquet", StringType.get()),
                      optional(18, "_some_nulls_variant", Types.VariantType.get()),
                      optional(19, "_all_nulls_variant", Types.VariantType.get())))));

  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;

  @Override
  protected void createInputFile() throws IOException {
    File parquetFile = new File(tempDir, "junit" + System.nanoTime());

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID());
    }
    String tooLongForParquetValue = sb.toString();

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<GenericRecord> appender =
        Parquet.write(outFile)
            .schema(PARQUET_FILE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        GenericRecord record = GenericRecord.create(PARQUET_FILE_SCHEMA);
        populateBaseFields(record, i);
        record.setField(
            "_no_stats_parquet",
            tooLongForParquetValue); // value longer than 4k will produce no stats
        if (i % 2 == 0) {
          VariantMetadata metadata = Variants.metadata("data");
          ShreddedObject obj = Variants.object(metadata);
          obj.put("data", Variants.of(25 + i));
          record.setField("_some_nulls_variant", Variant.of(metadata, obj));
        } else {
          record.setField("_some_nulls_variant", null);
        }
        record.setField("_all_nulls_variant", null);
        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      assertThat(reader.getRowGroups()).as("Should create only one row group").hasSize(1);
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }
    parquetFile.deleteOnExit();
  }

  @Override
  protected boolean shouldRead(Expression expression, boolean caseSensitive) {
    return shouldRead(expression, caseSensitive, rowGroupMetadata);
  }

  @Test
  public void testIsNaNNoNans() {
    boolean shouldRead = shouldRead(isNaN("no_nans"));
    assertThat(shouldRead).as("Parquet metrics don't track NaN counts").isTrue();
  }

  @Test
  public void testNotInNoNulls() {
    boolean shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    assertThat(shouldRead).as("Parquet metrics evaluator reads for notIn").isTrue();
  }

  @Test
  public void testColumnNotInFile() {
    Expression[] cannotMatch =
        new Expression[] {
          lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
          equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
          greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
        };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead)
          .as("Should skip when column is not in file (all nulls): " + expr)
          .isFalse();
    }

    Expression[] canMatch = new Expression[] {isNull("not_in_file"), notEqual("not_in_file", 1.0f)};

    for (Expression expr : canMatch) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead)
          .as("Should read when column is not in file (all nulls): " + expr)
          .isTrue();
    }
  }

  @Test
  public void testMissingStatsParquet() {
    Expression[] exprs =
        new Expression[] {
          lessThan("no_stats_parquet", "a"),
          lessThanOrEqual("no_stats_parquet", "b"),
          equal("no_stats_parquet", "c"),
          greaterThan("no_stats_parquet", "d"),
          greaterThanOrEqual("no_stats_parquet", "e"),
          notEqual("no_stats_parquet", "f"),
          isNull("no_stats_parquet"),
          notNull("no_stats_parquet"),
          startsWith("no_stats_parquet", "a"),
          notStartsWith("no_stats_parquet", "a")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead).as("Should read when missing stats for expr: " + expr).isTrue();
    }
  }

  @Test
  public void testZeroRecordFileParquet() {
    BlockMetaData emptyBlock = new BlockMetaData();
    emptyBlock.setRowCount(0);

    Expression[] exprs =
        new Expression[] {
          lessThan("id", 5),
          lessThanOrEqual("id", 30),
          equal("id", 70),
          greaterThan("id", 78),
          greaterThanOrEqual("id", 90),
          notEqual("id", 101),
          isNull("some_nulls"),
          notNull("some_nulls")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(expr, true, emptyBlock);
      assertThat(shouldRead).as("Should never read 0-record file: " + expr).isFalse();
    }
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead = shouldRead(startsWith("str", "1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "0st"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "1str1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "1str1_xgd"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "2str"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "9xstr"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "0S"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "x"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "9str9aaa"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();
  }

  @Test
  public void testStringNotStartsWith() {
    boolean shouldRead = shouldRead(notStartsWith("str", "1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "0st"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "1str1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "1str1_xgd"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "2str"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "9xstr"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("required", "r"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(notStartsWith("required", "requ"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("some_nulls", "ssome"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("some_nulls", "som"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();
  }

  @Test
  public void testInLimitParquet() {
    boolean shouldRead = shouldRead(in("id", 1, 2));
    assertThat(shouldRead).as("Should not read if IN is evaluated").isFalse();

    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }

    shouldRead = shouldRead(in("id", ids));
    assertThat(shouldRead).as("Should read if IN is not evaluated").isTrue();
  }

  @Test
  public void testParquetTypePromotion() {
    Schema promotedSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(promotedSchema, equal("id", INT_MIN_VALUE + 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    assertThat(shouldRead).as("Should succeed with promoted schema").isTrue();
  }

  @Test
  public void testTransformFilter() {
    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(
                PARQUET_SCHEMA, equal(truncate("required", 2), "some_value"), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    assertThat(shouldRead)
        .as("Should read: filter contains non-reference evaluate as True")
        .isTrue();
  }

  @Test
  public void testVariantNotNull() {
    boolean shouldRead = shouldRead(notNull("some_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant notNull filters must be evaluated post scan")
        .isTrue();

    shouldRead = shouldRead(notNull("all_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant notNull filters must be evaluated post scan even for all nulls")
        .isTrue();
  }

  @Test
  public void testVariantEq() {
    assertThatThrownBy(() -> shouldRead(equal("some_nulls_variant", "test")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test");
  }

  @Test
  public void testVariantIn() {
    assertThatThrownBy(() -> shouldRead(in("some_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");

    assertThatThrownBy(() -> shouldRead(in("all_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");
  }

  @Test
  public void testVariantNotIn() {
    assertThatThrownBy(() -> shouldRead(notIn("some_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");

    assertThatThrownBy(() -> shouldRead(notIn("all_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");
  }

  @Test
  public void testVariantIsNull() {
    boolean shouldRead = shouldRead(isNull("some_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant isNull filters must be evaluated post scan")
        .isTrue();

    shouldRead = shouldRead(isNull("all_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant isNull filters must be evaluated post scan even for all nulls")
        .isTrue();
  }

  @Test
  public void testVariantComparisons() {
    assertThatThrownBy(() -> shouldRead(lessThan("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(lessThanOrEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(greaterThan("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(greaterThanOrEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(notEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");
  }

  private boolean shouldRead(Expression expression, boolean caseSensitive, BlockMetaData block) {
    if (parquetSchema == null || block == null) {
      throw new IllegalStateException(
          "Parquet file not created yet. Call createInputFile() first.");
    }
    return new ParquetMetricsRowGroupFilter(PARQUET_SCHEMA, expression, caseSensitive)
        .shouldRead(parquetSchema, block);
  }

  private org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inFile.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream stream = inFile.newStream();
        return new DelegatingSeekableInputStream(stream) {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }
        };
      }
    };
  }
}

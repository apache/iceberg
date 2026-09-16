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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A pushed-down filter may return rows the predicate does not match — the caller re-applies the
 * filter — but it must never drop a row the predicate does match. Every case here asserts that
 * superset property against Iceberg's own {@link Evaluator}, which is the only way a wrong
 * translation shows up as anything other than silently missing data.
 */
public class TestVortexFilterPushdown {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "s", Types.StringType.get()),
          optional(3, "f", Types.DoubleType.get()));

  // Covers nulls, NaN, and the three characters that are special to Vortex's LIKE patterns.
  private static final List<Record> ROWS =
      List.of(
          row(1L, "alpha", 1.0d),
          row(2L, "al%pha", Double.NaN),
          row(3L, null, null),
          row(4L, "beta", 2.0d),
          row(5L, "al_pha", Double.NaN),
          row(6L, "al\\pha", 3.0d),
          row(7L, "al", 4.0d),
          row(8L, "ALPHA", 5.0d));

  @TempDir private Path temp;
  private InputFile file;

  private static Record row(long id, String text, Double value) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("s", text);
    record.setField("f", value);
    return record;
  }

  @BeforeEach
  public void writeFile() throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("pushdown-" + System.nanoTime() + ".vortex").toFile());
    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(SCHEMA)
            .content(FileContent.DATA)
            .build()) {
      appender.addAll(ROWS);
    }

    this.file = outputFile.toInputFile();
  }

  @Test
  public void testStartsWith() throws IOException {
    // "al" is a prefix of alpha, al%pha, al_pha, al\pha and al itself, but not of beta or ALPHA.
    assertPushdown(Expressions.startsWith("s", "al"), List.of(1L, 2L, 5L, 6L, 7L), true);
  }

  @Test
  public void testStartsWithEscapesLikeWildcards() throws IOException {
    // % and _ are Vortex LIKE wildcards and \ is its escape character. Unescaped, each of these
    // would match rows the predicate does not, and an unescaped backslash is dropped entirely.
    assertPushdown(Expressions.startsWith("s", "al%"), List.of(2L), true);
    assertPushdown(Expressions.startsWith("s", "al_"), List.of(5L), true);
    assertPushdown(Expressions.startsWith("s", "al\\"), List.of(6L), true);
  }

  @Test
  public void testNotStartsWith() throws IOException {
    // Iceberg's notStartsWith is the negation of startsWith, which is false for null, so the null
    // row matches. Vortex's NOT LIKE drops nulls, so they have to be added back.
    assertPushdown(Expressions.notStartsWith("s", "al"), List.of(3L, 4L, 8L), true);
  }

  @Test
  public void testComparisonsAndSets() throws IOException {
    assertPushdown(Expressions.greaterThan("id", 6L), List.of(7L, 8L), true);
    assertPushdown(Expressions.in("id", 1L, 4L), List.of(1L, 4L), true);
    assertPushdown(Expressions.notIn("id", 1L, 4L), List.of(2L, 3L, 5L, 6L, 7L, 8L), true);
    assertPushdown(Expressions.isNull("s"), List.of(3L), true);
    assertPushdown(Expressions.notNull("s"), List.of(1L, 2L, 4L, 5L, 6L, 7L, 8L), true);
  }

  @Test
  public void testNanPredicatesAreNotPushedDown() throws IOException {
    // Vortex compares NaN as equal to itself, so nothing isolates NaN. The filter must be dropped
    // rather than approximated: these rows have to survive the scan.
    assertPushdown(Expressions.isNaN("f"), List.of(2L, 5L), false);
    assertPushdown(Expressions.notNaN("f"), List.of(1L, 3L, 4L, 6L, 7L, 8L), false);
  }

  @Test
  public void testLargeInSetIsNotPushedDown() throws IOException {
    // Expanding a set this large costs more than the scan it saves, so the predicate is dropped
    // and the engine applies it. The scan returns a superset; every matching row must survive.
    // The values are distinct so the set really exceeds the expansion limit, and only 2 and 6
    // fall in the file's id range.
    Long[] values = new Long[300];
    values[0] = 2L;
    values[1] = 6L;
    for (int i = 2; i < values.length; i++) {
      values[i] = (long) (100 + i);
    }

    assertPushdown(Expressions.in("id", (Object[]) values), List.of(2L, 6L), false);
  }

  @Test
  public void testTransformPredicateIsDroppedNotFatal() throws IOException {
    // A transform term has no Vortex equivalent. It must degrade to "read everything", not throw.
    Expression filter = Expressions.equal(Expressions.bucket("id", 8), 3);
    assertThat(scan(filter)).containsAll(matching(filter));
  }

  /**
   * Asserts the scan returns every row the predicate matches. When {@code expectPruning} is set,
   * the scan must also return fewer rows than the file holds, proving the filter really was pushed
   * rather than silently dropped.
   */
  private void assertPushdown(Expression filter, List<Long> expected, boolean expectPruning)
      throws IOException {
    assertThat(matching(filter))
        .as("test expectation matches Iceberg's evaluator")
        .isEqualTo(expected);

    List<Long> scanned = scan(filter);
    assertThat(scanned).as("pushed filter must not drop matching rows").containsAll(expected);
    if (expectPruning) {
      assertThat(scanned).as("filter should have been pushed down").hasSizeLessThan(ROWS.size());
    }
  }

  private static List<Long> matching(Expression filter) {
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), filter);
    List<Long> ids = Lists.newArrayList();
    for (Record record : ROWS) {
      if (evaluator.eval(record)) {
        ids.add((Long) record.getField("id"));
      }
    }

    return ids;
  }

  private List<Long> scan(Expression filter) throws IOException {
    try (CloseableIterable<Record> records =
        formatModel().readBuilder(file).project(SCHEMA).filter(filter).build()) {
      List<Long> ids = Lists.newArrayList();
      for (Record record : records) {
        ids.add((Long) record.getField("id"));
      }

      return ids;
    }
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}

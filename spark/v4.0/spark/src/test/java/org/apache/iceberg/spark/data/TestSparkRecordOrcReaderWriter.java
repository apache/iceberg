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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;

public class TestSparkRecordOrcReaderWriter extends AvroDataTest {
  private static final int NUM_RECORDS = 200;

  private void writeAndValidate(Schema schema, List<Record> expectedRecords) throws IOException {
    final File originalFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(originalFile.delete()).as("Delete should succeed").isTrue();

    // Write few generic records into the original test file.
    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(originalFile))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(schema)
            .build()) {
      writer.addAll(expectedRecords);
    }

    // Read into spark InternalRow from the original test file.
    List<InternalRow> internalRows = Lists.newArrayList();
    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(originalFile))
            .project(schema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(schema, readOrcSchema))
            .build()) {
      reader.forEach(internalRows::add);
      assertEqualsUnsafe(schema.asStruct(), expectedRecords, reader, expectedRecords.size());
    }

    final File anotherFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(anotherFile.delete()).as("Delete should succeed").isTrue();

    // Write those spark InternalRows into a new file again.
    try (FileAppender<InternalRow> writer =
        ORC.write(Files.localOutput(anotherFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(schema)
            .build()) {
      writer.addAll(internalRows);
    }

    // Check whether the InternalRows are expected records.
    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(anotherFile))
            .project(schema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(schema, readOrcSchema))
            .build()) {
      assertEqualsUnsafe(schema.asStruct(), expectedRecords, reader, expectedRecords.size());
    }

    // Read into iceberg GenericRecord and check again.
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(anotherFile))
            .createReaderFunc(typeDesc -> GenericOrcReader.buildReader(schema, typeDesc))
            .project(schema)
            .build()) {
      assertRecordEquals(expectedRecords, reader, expectedRecords.size());
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expectedRecords = RandomGenericData.generate(schema, NUM_RECORDS, 1992L);
    writeAndValidate(schema, expectedRecords);
  }

  @Test
  public void testDecimalWithTrailingZero() throws IOException {
    Schema schema =
        new Schema(
            required(1, "d1", Types.DecimalType.of(10, 2)),
            required(2, "d2", Types.DecimalType.of(20, 5)),
            required(3, "d3", Types.DecimalType.of(38, 20)));

    List<Record> expected = Lists.newArrayList();

    GenericRecord record = GenericRecord.create(schema);
    record.set(0, new BigDecimal("101.00"));
    record.set(1, new BigDecimal("10.00E-3"));
    record.set(2, new BigDecimal("1001.0000E-16"));

    expected.add(record.copy());

    writeAndValidate(schema, expected);
  }

  private static void assertRecordEquals(
      Iterable<Record> expected, Iterable<Record> actual, int size) {
    Iterator<Record> expectedIter = expected.iterator();
    Iterator<Record> actualIter = actual.iterator();
    for (int i = 0; i < size; i += 1) {
      assertThat(expectedIter).as("Expected iterator should have more rows").hasNext();
      assertThat(actualIter).as("Actual iterator should have more rows").hasNext();
      assertThat(actualIter.next()).as("Should have same rows.").isEqualTo(expectedIter.next());
    }
    assertThat(expectedIter).as("Expected iterator should not have any extra rows.").isExhausted();
    assertThat(actualIter).as("Actual iterator should not have any extra rows.").isExhausted();
  }

  private static void assertEqualsUnsafe(
      Types.StructType struct, Iterable<Record> expected, Iterable<InternalRow> actual, int size) {
    Iterator<Record> expectedIter = expected.iterator();
    Iterator<InternalRow> actualIter = actual.iterator();
    for (int i = 0; i < size; i += 1) {
      assertThat(expectedIter).as("Expected iterator should have more rows").hasNext();
      assertThat(actualIter).as("Actual iterator should have more rows").hasNext();
      GenericsHelpers.assertEqualsUnsafe(struct, expectedIter.next(), actualIter.next());
    }
    assertThat(expectedIter).as("Expected iterator should not have any extra rows.").isExhausted();
    assertThat(actualIter).as("Actual iterator should not have any extra rows.").isExhausted();
  }
}

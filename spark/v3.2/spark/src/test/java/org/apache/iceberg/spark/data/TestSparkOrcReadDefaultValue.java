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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.source.BaseDataReader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkOrcReadDefaultValue {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testOrcDefaultValues() throws IOException {

    Schema writeSchema = new Schema(
        required(1, "col1", Types.IntegerType.get())
    );
    List<InternalRow> data = Collections.nCopies(
        100,
        RandomData.generateSpark(writeSchema, 1, 0L).iterator().next());

    Type[] defaultValueTypes = {
        Types.StringType.get(),
        Types.ListType.ofRequired(10, Types.IntegerType.get()),
        Types.MapType.ofRequired(11, 12, Types.StringType.get(),
            Types.IntegerType.get()),
        Types.StructType.of(
            Types.NestedField.required(13, "nested_col1", Types.IntegerType.get())),
        Types.ListType.ofRequired(14, Types.StructType.of(
            Types.NestedField.required(15, "nested_col2", Types.StringType.get())))
    };

    Object[] defaultValues = {
        "foo",  // string default
        ImmutableList.of(1, 2), // array default
        ImmutableMap.of("bar", 1), // map default
        ImmutableMap.of(13, 1), // struct default
        ImmutableList.of(ImmutableMap.of(15, "xyz"), ImmutableMap.of(15, "baz")) // array of struct default
    };

    Object[] expectedDefaults =
        IntStream.range(0, defaultValues.length).mapToObj(i -> BaseDataReader.convertConstant(
            defaultValueTypes[i],
            defaultValues[i])).toArray();

    List<InternalRow> expected = data.stream().map(internalRow -> {
      Object[] values = new Object[6];
      values[0] = internalRow.getInt(0);
      System.arraycopy(expectedDefaults, 0, values, 1, expectedDefaults.length);
      return new GenericInternalRow(values);
    }).collect(Collectors.toList());

    // evolve the schema to add col2, col3, col4, col5, col6 with default value
    List<Types.NestedField> newSchemaFields = Lists.newArrayList(writeSchema.findField(1));
    newSchemaFields.addAll(IntStream.range(0, defaultValues.length).mapToObj(i -> Types.NestedField.required(i + 2,
        String.format("col%d", i + 2), defaultValueTypes[i], "doc", defaultValues[i], defaultValues[i])).collect(
        Collectors.toList()));
    Schema readSchema = new Schema(newSchemaFields);

    final File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(writeSchema)
        .build()) {
      writer.addAll(data);
    }

    // Try to read back the data using the evolved readSchema, the returned data should
    // have default values populated in them.

    // non-vectorized read
    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(testFile))
        .project(readSchema)
        .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
        .build()) {

      final Iterator<InternalRow> actualRows = reader.iterator();
      final Iterator<InternalRow> expectedRows = expected.iterator();
      while (expectedRows.hasNext()) {
        Assert.assertTrue("Should have expected number of rows", actualRows.hasNext());
        assertEquals(readSchema, expectedRows.next(), actualRows.next());
      }
      Assert.assertFalse("Should not have extra rows", actualRows.hasNext());
    }

    // vectorized-read
    // try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
    //     .project(readSchema)
    //     .createBatchedReaderFunc(readOrcSchema ->
    //         VectorizedSparkOrcReaders.buildReader(readSchema, readOrcSchema, ImmutableMap.of()))
    //     .build()) {
    //   final Iterator<InternalRow> actualRows = batchesToRows(reader.iterator());
    //   final InternalRow actualFirstRow = actualRows.next();
    //
    //   assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    // }
  }

  private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
    return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
  }
}

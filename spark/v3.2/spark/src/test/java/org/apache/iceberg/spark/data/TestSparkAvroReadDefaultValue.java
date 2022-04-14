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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkAvroReadDefaultValue {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testAvroDefaultValues() throws IOException {
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

    // evolve the schema to add col2, col3, col4, col5, col6 with default value
    List<Types.NestedField> newSchemaFields = Lists.newArrayList(writeSchema.findField(1));
    newSchemaFields.addAll(IntStream.range(0, defaultValues.length).mapToObj(i -> Types.NestedField.required(i + 2,
        String.format("col%d", i + 2), defaultValueTypes[i], "doc", defaultValues[i], defaultValues[i])).collect(
        Collectors.toList()));
    Schema readSchema = new Schema(newSchemaFields);

    // Construct the expected read-back data as avro record
    GenericData.Record r1 = new GenericData.Record(AvroSchemaUtil.convert(defaultValueTypes[3]));
    r1.put(0, 1);
    GenericData.Record r2 =
        new GenericData.Record(AvroSchemaUtil.convert(defaultValueTypes[4].asListType().elementType()));
    GenericData.Record r3 =
        new GenericData.Record(AvroSchemaUtil.convert(defaultValueTypes[4].asListType().elementType()));
    r2.put(0, "xyz");
    r3.put(0, "baz");
    Object[] avroDefaultValues = {
        "foo",  // string default
        ImmutableList.of(1, 2), // array default
        ImmutableMap.of("bar", 1), // map default
        r1, // struct default
        ImmutableList.of(r2, r3) // array of struct default
    };

    List<GenericData.Record> expectedRecords = data.stream().map(datum -> {
      GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(readSchema.asStruct()));
      record.put(0, datum.getInt(0));
      for (int j = 0; j < avroDefaultValues.length; j++) {
        record.put(j + 1, avroDefaultValues[j]);
      }
      return record;
    }).collect(Collectors.toList());

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (
        FileAppender<InternalRow> writer = Avro.write(Files.localOutput(testFile))
            .createWriterFunc(avroSchema -> new SparkAvroWriter(SparkSchemaUtil.convert(writeSchema)))
            .schema(writeSchema)
            .named("test")
            .build()) {
      writer.addAll(data);
    }

    List<InternalRow> actualRows;
    try (
        AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
            .createReaderFunc(SparkAvroReader::new)
            .project(readSchema)
            .build()) {
      actualRows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expectedRecords.size(); i += 1) {
      assertEqualsUnsafe(readSchema.asStruct(), expectedRecords.get(i), actualRows.get(i));
    }
  }
}

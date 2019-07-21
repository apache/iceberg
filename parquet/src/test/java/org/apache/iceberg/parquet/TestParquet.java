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

package org.apache.iceberg.parquet;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestParquet extends BaseParquetWritingTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRowGroupSizeConfigurable() throws IOException {
    // Without an explicit writer function
    File parquetFile = generateFileWithTwoRowGroups(null);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      Assert.assertEquals(2, reader.getRowGroups().size());
    }
  }

  @Test
  public void testRowGroupSizeConfigurableWithWriter() throws IOException {
    File parquetFile = generateFileWithTwoRowGroups(ParquetAvroWriter::buildWriter);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
      Assert.assertEquals(2, reader.getRowGroups().size());
    }
  }

  @Test
  public void testCreateModeWithoutWriterFunc() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.PARQUET_WRITE_MODE, "create");

    File file = write(null, properties, 10, null);

    thrown.expect(AlreadyExistsException.class);
    write(null, properties, 5, file);
  }

  @Test
  public void testCreateModeWithWriterFunc() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.PARQUET_WRITE_MODE, "create");

    File file = write(ParquetAvroWriter::buildWriter, properties, 10, null);

    thrown.expect(AlreadyExistsException.class);
    write(ParquetAvroWriter::buildWriter, properties, 5, file);
  }

  @Test
  public void testOverwriteModeWithoutWriterFunc() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.PARQUET_WRITE_MODE, "overwrite");

    File file = write(null, properties, 10, null);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)))) {
      Assert.assertEquals(10, reader.getRecordCount());
    }

    write(null, properties, 5, file);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)))) {
      Assert.assertEquals(5, reader.getRecordCount());
    }
  }

  @Test
  public void testOverwriteModeWithWriterFunc() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.PARQUET_WRITE_MODE, "overwrite");

    File file = write(ParquetAvroWriter::buildWriter, properties, 10, null);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)))) {
      Assert.assertEquals(10, reader.getRecordCount());
    }

    write(ParquetAvroWriter::buildWriter, properties, 5, file);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)))) {
      Assert.assertEquals(5, reader.getRecordCount());
    }
  }

  @Test
  public void testUnsupportedWriteMode() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    write(null, ImmutableMap.of(TableProperties.PARQUET_WRITE_MODE, "abc"), 10, null);
  }

  private File generateFileWithTwoRowGroups(Function<MessageType, ParquetValueWriter<?>> createWriterFunc)
      throws IOException {
    int minimumRowGroupRecordCount = 100;
    int desiredRecordCount = minimumRowGroupRecordCount + 1;

    // Force multiple row groups by making the byte size very small
    // Note there'a also minimumRowGroupRecordCount which cannot be configured so we have to write
    // at least that many records for a new row group to occur
    Map<String, String> properties = ImmutableMap.of(
      PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(minimumRowGroupRecordCount * Integer.BYTES));

    return write(createWriterFunc, properties, desiredRecordCount, null);
  }

  private File write(Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
                     Map<String, String> properties,
                     int recordsNumber,
                     File file) throws IOException {
    Schema schema = new Schema(
        optional(1, "intCol", IntegerType.get())
    );

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    List<GenericData.Record> records = IntStream.rangeClosed(1, recordsNumber)
        .mapToObj(index -> {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put("intCol", index);
          return record;
        })
        .collect(Collectors.toList());

    return writeRecords(schema, properties, createWriterFunc, file, records.toArray(new GenericData.Record[] {}));
  }
}

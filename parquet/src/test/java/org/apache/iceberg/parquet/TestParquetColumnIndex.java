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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.Parquet.ReadBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.writeRecords;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestParquetColumnIndex {

  private MessageType parquetSchema = new MessageType("schema",
      new PrimitiveType(REQUIRED, INT32, "intCol"));
  private Schema schema = ParquetSchemaUtil.convert(parquetSchema);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testColumnIndexFilter() throws IOException {
    File parquetFile = generateFileWithMultiplePages(ParquetAvroWriter::buildWriter);
    int totalCount = getPageRecordCount(parquetFile, null);
    int filterCount = getPageRecordCount(parquetFile,
            Expressions.and(Expressions.notNull("intCol"), Expressions.equal("intCol", 1)));
    Assert.assertTrue(filterCount < totalCount);
  }

  private int getPageRecordCount(File parquetFile, Expression expr) {
    List<Object> records = new ArrayList<>();
    ReadBuilder builder = Parquet.read(localInput(parquetFile))
            .project(schema)
            .filterRecords(true)
            .filter(expr)
             .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema));
    if (expr != null) {
      builder.filterRecords(true).filter(expr);
    }
    CloseableIterator iter = builder.build().iterator();
    while (iter.hasNext()) {
      records.add(iter.next());
    }
    return records.size();
  }

  private File generateFileWithMultiplePages(Function<MessageType, ParquetValueWriter<?>> createWriterFunc)
      throws IOException {

    int recordNum = 1000000;
    List<GenericData.Record> records = new ArrayList<>(recordNum);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= recordNum; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put("intCol", i);
      records.add(record);
    }

    // We make it 1000 pages, so that we can skip some
    return writeRecords(temp,
            schema,
            ImmutableMap.of(
                    PARQUET_PAGE_SIZE_BYTES,
                    Integer.toString((recordNum / 1000) * Integer.BYTES)),
            createWriterFunc,
            records.toArray(new GenericData.Record[] {}));
  }
}

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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.ParquetFileTestUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

public class TestVariantShreddingWrite extends WriterTestBase {

  private static final org.apache.iceberg.Schema VARIANT_TABLE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "v", Types.VariantType.get()));

  @Test
  public void testKafkaConnectVariantShreddingRoundTrip() throws IOException {
    when(table.schema()).thenReturn(VARIANT_TABLE_SCHEMA);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps())
        .thenReturn(
            ImmutableMap.of(
                "write.format.default",
                "parquet",
                TableProperties.PARQUET_SHRED_VARIANTS,
                "true",
                TableProperties.PARQUET_VARIANT_BUFFER_SIZE,
                "2"));

    RecordConverter converter = new RecordConverter(table, config);

    Schema connectSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("v", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();
    Struct struct1 =
        new Struct(connectSchema).put("id", 1L).put("v", ImmutableMap.of("a", 1, "b", 2));
    Struct struct2 =
        new Struct(connectSchema).put("id", 2L).put("v", ImmutableMap.of("a", 3, "b", 4));

    Record row1 = converter.convert(struct1);
    Record row2 = converter.convert(struct2);

    WriteResult result = writeTest(ImmutableList.of(row1, row2), config, UnpartitionedWriter.class);

    assertThat(result.dataFiles()).hasSize(1);
    DataFile dataFile = result.dataFiles()[0];

    try (ParquetFileReader reader =
        ParquetFileReader.open(
            ParquetFileTestUtils.file(fileIO.newInputFile(dataFile.location())))) {
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      GroupType variantGroup = parquetSchema.getType("v").asGroupType();
      assertThat(variantGroup.containsField("typed_value")).isTrue();

      GroupType typedValue = variantGroup.getType("typed_value").asGroupType();
      assertThat(typedValue.containsField("a")).isTrue();
      assertThat(typedValue.containsField("b")).isTrue();
    }
  }
}

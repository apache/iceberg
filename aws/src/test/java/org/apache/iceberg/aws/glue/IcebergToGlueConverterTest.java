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

package org.apache.iceberg.aws.glue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

public class IcebergToGlueConverterTest {

  @Test
  public void toDatabaseName() {
    Assert.assertEquals("db", IcebergToGlueConverter.toDatabaseName(Namespace.of("db")));
  }

  @Test
  public void toDatabaseName_fail() {
    List<Namespace> badNames = Lists.newArrayList(
        Namespace.of("db", "a"),
        Namespace.of("db-1"),
        Namespace.empty(),
        Namespace.of(""),
        Namespace.of(new String(new char[600]).replace("\0", "a")));
    for (Namespace name : badNames) {
      AssertHelpers.assertThrows("bad namespace name",
          ValidationException.class,
          "Cannot convert namespace",
          () -> IcebergToGlueConverter.toDatabaseName(name)
      );
    }
  }

  @Test
  public void toDatabaseInput() {
    Map<String, String> param = new HashMap<>();
    DatabaseInput input = DatabaseInput.builder()
        .name("db")
        .parameters(param)
        .build();
    Namespace namespace = Namespace.of("db");
    Assert.assertEquals(input, IcebergToGlueConverter.toDatabaseInput(namespace, param));
  }

  @Test
  public void testSetTableInputInformation() {
    // Actual TableInput
    TableInput.Builder actualTableInputBuilder = TableInput.builder();
    Schema schema = new Schema(
        Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"),
        Types.NestedField.required(2, "y", Types.StructType.of(
            Types.NestedField.required(3, "z", Types.IntegerType.get())), "comment2")
    );
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
        .identity("x")
        .withSpecId(1000)
        .build();
    TableMetadata tableMetadata = TableMetadata
        .newTableMetadata(schema, partitionSpec, "s3://test", ImmutableMap.of());
    IcebergToGlueConverter.setTableInputInformation(actualTableInputBuilder, tableMetadata);
    TableInput actualTableInput = actualTableInputBuilder.build();

    // Expected TableInput
    TableInput expectedTableInput = TableInput.builder().storageDescriptor(
        StorageDescriptor.builder()
            .location("s3://test")
            .columns(ImmutableList.of(
                Column.builder()
                    .name("x")
                    .type("string")
                    .comment("comment1")
                    .parameters(ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_USAGE, IcebergToGlueConverter.SCHEMA_COLUMN,
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_STRING, "string",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_TYPE_ID, "STRING"
                    ))
                    .build(),
                Column.builder()
                    .name("y")
                    .type("struct<z:int>")
                    .comment("comment2")
                    .parameters(ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_USAGE, IcebergToGlueConverter.SCHEMA_COLUMN,
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_STRING, "struct<z:int>",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_TYPE_ID, "STRUCT"
                    ))
                    .build(),
                Column.builder()
                    .name("z")
                    .type("int")
                    .parameters(ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_USAGE, IcebergToGlueConverter.SCHEMA_SUBFIELD,
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "3",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_STRING, "int",
                        IcebergToGlueConverter.ICEBERG_FIELD_TYPE_TYPE_ID, "INTEGER"
                    ))
                    .build(),
                Column.builder()
                    .name("x")
                    .type("string")
                    .parameters(ImmutableMap.<String, String>builder()
                        .put(IcebergToGlueConverter.ICEBERG_FIELD_USAGE, IcebergToGlueConverter.PARTITION_FIELD)
                        .put(IcebergToGlueConverter.ICEBERG_FIELD_TYPE_TYPE_ID, "STRING")
                        .put(IcebergToGlueConverter.ICEBERG_FIELD_TYPE_STRING, "string")
                        .put(IcebergToGlueConverter.ICEBERG_FIELD_ID, "1000")
                        .put(IcebergToGlueConverter.ICEBERG_PARTITION_FIELD_ID, "1000")
                        .put(IcebergToGlueConverter.ICEBERG_PARTITION_SOURCE_ID, "1")
                        .put(IcebergToGlueConverter.ICEBERG_PARTITION_TRANSFORM, "identity")
                        .build()
                    )
                    .build()
                )
            ).build())
        .build();

    Assert.assertEquals(
        "Location do not match",
        expectedTableInput.storageDescriptor().location(),
        actualTableInput.storageDescriptor().location());
    Assert.assertEquals(
        "Columns do not match",
        expectedTableInput.storageDescriptor().columns(),
        actualTableInput.storageDescriptor().columns());
  }
}

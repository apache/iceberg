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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

public class TestIcebergToGlueConverter {

  private final Map<String, String> tableLocationProperties =
      ImmutableMap.of(
          TableProperties.WRITE_DATA_LOCATION, "s3://writeDataLoc",
          TableProperties.WRITE_METADATA_LOCATION, "s3://writeMetaDataLoc",
          TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://writeFolderStorageLoc");

  @Test
  public void testToDatabaseName() {
    Assert.assertEquals("db", IcebergToGlueConverter.toDatabaseName(Namespace.of("db"), false));
  }

  @Test
  public void testToDatabaseNameFailure() {
    List<Namespace> badNames =
        Lists.newArrayList(
            Namespace.of("db", "a"),
            Namespace.of("db-1"),
            Namespace.empty(),
            Namespace.of(""),
            Namespace.of(new String(new char[600]).replace("\0", "a")));
    for (Namespace name : badNames) {
      AssertHelpers.assertThrows(
          "bad namespace name",
          ValidationException.class,
          "Cannot convert namespace",
          () -> IcebergToGlueConverter.toDatabaseName(name, false));
    }
  }

  @Test
  public void testSkipNamespaceValidation() {
    List<Namespace> acceptableNames =
        Lists.newArrayList(Namespace.of("db-1"), Namespace.of("db-1-1-1"));
    for (Namespace name : acceptableNames) {
      Assert.assertEquals(name.toString(), IcebergToGlueConverter.toDatabaseName(name, true));
    }
  }

  @Test
  public void testSkipTableNameValidation() {
    List<TableIdentifier> acceptableIdentifiers =
        Lists.newArrayList(
            TableIdentifier.parse("db.a-1"),
            TableIdentifier.parse("db.a-1-1"),
            TableIdentifier.parse("db.a#1"));
    for (TableIdentifier identifier : acceptableIdentifiers) {
      Assert.assertEquals(identifier.name(), IcebergToGlueConverter.getTableName(identifier, true));
    }
  }

  @Test
  public void testToDatabaseInput() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergToGlueConverter.GLUE_DB_DESCRIPTION_KEY,
            "description",
            IcebergToGlueConverter.GLUE_DB_LOCATION_KEY,
            "s3://location",
            "key",
            "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    Assert.assertEquals("Location should be set", "s3://location", databaseInput.locationUri());
    Assert.assertEquals("Description should be set", "description", databaseInput.description());
    Assert.assertEquals(
        "Parameters should be set", ImmutableMap.of("key", "val"), databaseInput.parameters());
    Assert.assertEquals("Database name should be set", "ns", databaseInput.name());
  }

  @Test
  public void testToDatabaseInputNoParameter() {
    DatabaseInput input = DatabaseInput.builder().name("db").parameters(ImmutableMap.of()).build();
    Namespace namespace = Namespace.of("db");
    Assert.assertEquals(
        input, IcebergToGlueConverter.toDatabaseInput(namespace, ImmutableMap.of(), false));
  }

  @Test
  public void testToDatabaseInputEmptyLocation() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergToGlueConverter.GLUE_DB_DESCRIPTION_KEY, "description", "key", "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    Assert.assertNull("Location should not be set", databaseInput.locationUri());
    Assert.assertEquals("Description should be set", "description", databaseInput.description());
    Assert.assertEquals(
        "Parameters should be set", ImmutableMap.of("key", "val"), databaseInput.parameters());
    Assert.assertEquals("Database name should be set", "ns", databaseInput.name());
  }

  @Test
  public void testToDatabaseInputEmptyDescription() {
    Map<String, String> properties =
        ImmutableMap.of(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://location", "key", "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    Assert.assertEquals("Location should be set", "s3://location", databaseInput.locationUri());
    Assert.assertNull("Description should not be set", databaseInput.description());
    Assert.assertEquals(
        "Parameters should be set", ImmutableMap.of("key", "val"), databaseInput.parameters());
    Assert.assertEquals("Database name should be set", "ns", databaseInput.name());
  }

  @Test
  public void testSetTableInputInformation() {
    // Actual TableInput
    TableInput.Builder actualTableInputBuilder = TableInput.builder();
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"),
            Types.NestedField.required(
                2,
                "y",
                Types.StructType.of(Types.NestedField.required(3, "z", Types.IntegerType.get())),
                "comment2"));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("x").withSpecId(1000).build();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(schema, partitionSpec, "s3://test", tableLocationProperties);
    IcebergToGlueConverter.setTableInputInformation(actualTableInputBuilder, tableMetadata);
    TableInput actualTableInput = actualTableInputBuilder.build();

    // Expected TableInput
    TableInput expectedTableInput =
        TableInput.builder()
            .storageDescriptor(
                StorageDescriptor.builder()
                    .location("s3://test")
                    .additionalLocations(Sets.newHashSet(tableLocationProperties.values()))
                    .columns(
                        ImmutableList.of(
                            Column.builder()
                                .name("x")
                                .type("string")
                                .comment("comment1")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build(),
                            Column.builder()
                                .name("y")
                                .type("struct<z:int>")
                                .comment("comment2")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build()))
                    .build())
            .build();

    Assert.assertEquals(
        "additionalLocations should match",
        expectedTableInput.storageDescriptor().additionalLocations(),
        actualTableInput.storageDescriptor().additionalLocations());
    Assert.assertEquals(
        "Location should match",
        expectedTableInput.storageDescriptor().location(),
        actualTableInput.storageDescriptor().location());
    Assert.assertEquals(
        "Columns should match",
        expectedTableInput.storageDescriptor().columns(),
        actualTableInput.storageDescriptor().columns());
  }

  @Test
  public void testSetTableInputInformationWithRemovedColumns() {
    // Actual TableInput
    TableInput.Builder actualTableInputBuilder = TableInput.builder();
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"),
            Types.NestedField.required(
                2,
                "y",
                Types.StructType.of(Types.NestedField.required(3, "z", Types.IntegerType.get())),
                "comment2"));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("x").withSpecId(1000).build();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(schema, partitionSpec, "s3://test", tableLocationProperties);

    Schema newSchema =
        new Schema(Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"));
    tableMetadata = tableMetadata.updateSchema(newSchema, 3);
    IcebergToGlueConverter.setTableInputInformation(actualTableInputBuilder, tableMetadata);
    TableInput actualTableInput = actualTableInputBuilder.build();

    // Expected TableInput
    TableInput expectedTableInput =
        TableInput.builder()
            .storageDescriptor(
                StorageDescriptor.builder()
                    .additionalLocations(Sets.newHashSet(tableLocationProperties.values()))
                    .location("s3://test")
                    .columns(
                        ImmutableList.of(
                            Column.builder()
                                .name("x")
                                .type("string")
                                .comment("comment1")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build(),
                            Column.builder()
                                .name("y")
                                .type("struct<z:int>")
                                .comment("comment2")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "false"))
                                .build()))
                    .build())
            .build();

    Assert.assertEquals(
        "additionalLocations should match",
        expectedTableInput.storageDescriptor().additionalLocations(),
        actualTableInput.storageDescriptor().additionalLocations());
    Assert.assertEquals(
        "Location should match",
        expectedTableInput.storageDescriptor().location(),
        actualTableInput.storageDescriptor().location());
    Assert.assertEquals(
        "Columns should match",
        expectedTableInput.storageDescriptor().columns(),
        actualTableInput.storageDescriptor().columns());
  }
}

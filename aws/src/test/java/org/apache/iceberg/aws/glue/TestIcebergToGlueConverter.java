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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

public class TestIcebergToGlueConverter {

  private final Map<String, String> tableLocationProperties =
      ImmutableMap.of(
          TableProperties.WRITE_DATA_LOCATION, "s3://writeDataLoc",
          TableProperties.WRITE_METADATA_LOCATION, "s3://writeMetaDataLoc",
          TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://writeFolderStorageLoc");

  @Test
  public void testToDatabaseName() {
    assertThat(IcebergToGlueConverter.toDatabaseName(Namespace.of("db"), false)).isEqualTo("db");
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

      assertThatThrownBy(() -> IcebergToGlueConverter.toDatabaseName(name, false))
          .isInstanceOf(ValidationException.class)
          .hasMessageStartingWith("Cannot convert namespace")
          .hasMessageEndingWith(
              "to Glue database name, "
                  + "because it must be 1-252 chars of lowercase letters, numbers, underscore");
    }
  }

  @Test
  public void testSkipNamespaceValidation() {
    List<Namespace> acceptableNames =
        Lists.newArrayList(Namespace.of("db-1"), Namespace.of("db-1-1-1"));
    for (Namespace name : acceptableNames) {
      assertThat(IcebergToGlueConverter.toDatabaseName(name, true)).isEqualTo(name.toString());
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
      assertThat(IcebergToGlueConverter.getTableName(identifier, true))
          .isEqualTo(identifier.name());
    }
  }

  @Test
  public void testToDatabaseInput() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergToGlueConverter.GLUE_DESCRIPTION_KEY,
            "description",
            IcebergToGlueConverter.GLUE_DB_LOCATION_KEY,
            "s3://location",
            "key",
            "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    assertThat(databaseInput.locationUri()).as("Location should be set").isEqualTo("s3://location");
    assertThat(databaseInput.description())
        .as("Description should be set")
        .isEqualTo("description");
    assertThat(databaseInput.parameters())
        .as("Parameters should be set")
        .isEqualTo(ImmutableMap.of("key", "val"));
    assertThat(databaseInput.name()).as("Database name should be set").isEqualTo("ns");
  }

  @Test
  public void testToDatabaseInputNoParameter() {
    DatabaseInput input = DatabaseInput.builder().name("db").parameters(ImmutableMap.of()).build();
    Namespace namespace = Namespace.of("db");
    assertThat(IcebergToGlueConverter.toDatabaseInput(namespace, ImmutableMap.of(), false))
        .isEqualTo(input);
  }

  @Test
  public void testToDatabaseInputEmptyLocation() {
    Map<String, String> properties =
        ImmutableMap.of(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, "description", "key", "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    assertThat(databaseInput.locationUri()).as("Location should not be set").isNull();
    assertThat(databaseInput.description())
        .as("Description should be set")
        .isEqualTo("description");
    assertThat(databaseInput.parameters())
        .as("Parameters should be set")
        .isEqualTo(ImmutableMap.of("key", "val"));
    assertThat(databaseInput.name()).as("Database name should be set").isEqualTo("ns");
  }

  @Test
  public void testToDatabaseInputEmptyDescription() {
    Map<String, String> properties =
        ImmutableMap.of(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://location", "key", "val");
    DatabaseInput databaseInput =
        IcebergToGlueConverter.toDatabaseInput(Namespace.of("ns"), properties, false);
    assertThat(databaseInput.locationUri()).as("Location should be set").isEqualTo("s3://location");
    assertThat(databaseInput.description()).as("Description should not be set").isNull();
    assertThat(databaseInput.parameters())
        .as("Parameters should be set")
        .isEqualTo(ImmutableMap.of("key", "val"));
    assertThat(databaseInput.name()).as("Database name should be set").isEqualTo("ns");
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

    assertThat(actualTableInput.storageDescriptor().additionalLocations())
        .as("additionalLocations should match")
        .isEqualTo(expectedTableInput.storageDescriptor().additionalLocations());
    assertThat(actualTableInput.storageDescriptor().location())
        .as("Location should match")
        .isEqualTo(expectedTableInput.storageDescriptor().location());
    assertThat(actualTableInput.storageDescriptor().columns())
        .as("Columns should match")
        .isEqualTo(expectedTableInput.storageDescriptor().columns());
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

    assertThat(actualTableInput.storageDescriptor().additionalLocations())
        .as("additionalLocations should match")
        .isEqualTo(expectedTableInput.storageDescriptor().additionalLocations());
    assertThat(actualTableInput.storageDescriptor().location())
        .as("Location should match")
        .isEqualTo(expectedTableInput.storageDescriptor().location());
    assertThat(actualTableInput.storageDescriptor().columns())
        .as("Columns should match")
        .isEqualTo(expectedTableInput.storageDescriptor().columns());
  }

  @Test
  public void testSetTableDescription() {
    String tableDescription = "hello world!";
    Map<String, String> tableProperties =
        ImmutableMap.<String, String>builder()
            .putAll((tableLocationProperties))
            .put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, tableDescription)
            .build();
    TableInput.Builder actualTableInputBuilder = TableInput.builder();
    Schema schema =
        new Schema(Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"));
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "s3://test", tableProperties);

    IcebergToGlueConverter.setTableInputInformation(actualTableInputBuilder, tableMetadata);
    TableInput actualTableInput = actualTableInputBuilder.build();

    assertThat(actualTableInput.description())
        .as("description should match")
        .isEqualTo(tableDescription);
  }

  @Test
  public void testSetTableInputInformationWithExistingTable() {
    // Actual TableInput
    TableInput.Builder actualTableInputBuilder = TableInput.builder();
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.StringType.get()),
            Types.NestedField.required(2, "y", Types.StringType.get(), "new comment"),
            Types.NestedField.required(3, "z", Types.StringType.get(), "new comment"));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("x").withSpecId(1000).build();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(schema, partitionSpec, "s3://test", tableLocationProperties);

    // Existing Table
    Table existingGlueTable =
        Table.builder()
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(
                        ImmutableList.of(
                            Column.builder().name("x").comment("existing comment").build(),
                            Column.builder().name("y").comment("existing comment").build()))
                    .build())
            .build();

    IcebergToGlueConverter.setTableInputInformation(
        actualTableInputBuilder, tableMetadata, existingGlueTable);
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
                                .comment("existing comment")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build(),
                            Column.builder()
                                .name("y")
                                .type("string")
                                .comment("new comment")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build(),
                            Column.builder()
                                .name("z")
                                .type("string")
                                .comment("new comment")
                                .parameters(
                                    ImmutableMap.of(
                                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "3",
                                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                                .build()))
                    .build())
            .build();

    assertThat(actualTableInput.storageDescriptor().columns())
        .as("Columns should match")
        .isEqualTo(expectedTableInput.storageDescriptor().columns());
  }
}

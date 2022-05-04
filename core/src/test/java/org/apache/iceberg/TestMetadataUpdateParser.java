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

package org.apache.iceberg;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataUpdateParser {

  private static final Schema ID_DATA_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  public void testMetadataUpdateWithoutActionCannotDeserialize() {
    List<String> invalidJson = ImmutableList.of(
        "{\"action\":null,\"format-version\":2}",
        "{\"format-version\":2}"
    );

    for (String json : invalidJson) {
      AssertHelpers.assertThrows(
          "MetadataUpdate without a recognized action should fail to deserialize",
          IllegalArgumentException.class,
          "Cannot parse metadata update. Missing field: action",
          () -> MetadataUpdateParser.fromJson(json));
    }
  }

  @Test
  public void testUpgradeFormatVersionToJson() {
    int formatVersion = 2;
    String action = MetadataUpdateParser.UPGRADE_FORMAT_VERSION;
    String json = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion expected = new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testUpgradeFormatVersionFromJson() {
    int formatVersion = 2;
    String expected = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion actual = new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    Assert.assertEquals("Upgrade format version should convert to the correct JSON value",
        expected, MetadataUpdateParser.toJson(actual));
  }

  @Test
  public void testAddSchemaFromJson() {
    String action = MetadataUpdateParser.ADD_SCHEMA;
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String json = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
        SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate actualUpdate = new MetadataUpdate.AddSchema(schema, lastColumnId);
    assertEquals(action, actualUpdate, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddSchemaToJson() {
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String expected = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
        SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate update = new MetadataUpdate.AddSchema(schema, lastColumnId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Add schema should convert to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetCurrentSchemaFromJson() {
    String action = MetadataUpdateParser.SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String json = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate.SetCurrentSchema expected = new MetadataUpdate.SetCurrentSchema(schemaId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetCurrentSchemaToJson() {
    String action = MetadataUpdateParser.SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String expected = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate update = new MetadataUpdate.SetCurrentSchema(schemaId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Set current schema should convert to the correct JSON value", expected, actual);
  }

  @Test
  public void testAddPartitionSpecFromJsonWithFieldId() {
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString = "{" +
        "\"spec-id\":1," +
        "\"fields\":[{" +
        "\"name\":\"id_bucket\"," +
        "\"transform\":\"bucket[8]\"," +
        "\"source-id\":1," +
        "\"field-id\":1000" +
        "},{" +
        "\"name\":\"data_bucket\"," +
        "\"transform\":\"bucket[16]\"," +
        "\"source-id\":2," +
        "\"field-id\":1001" +
        "}]" +
        "}";

    UnboundPartitionSpec actualSpec = PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String json = String.format("{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    // Partition spec order declaration needs to match declaration in spec string to be assigned the same field ids.
    PartitionSpec expectedSpec = PartitionSpec.builderFor(ID_DATA_SCHEMA)
        .bucket("id", 8)
        .bucket("data", 16)
        .withSpecId(1)
        .build();
    MetadataUpdate expected = new MetadataUpdate.AddPartitionSpec(expectedSpec);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddPartitionSpecFromJsonWithoutFieldId() {
    // partition field ids are missing in old PartitionSpec, they always auto-increment from 1000 in declared order
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString = "{" +
        "\"spec-id\":1," +
        "\"fields\":[{" +
        "\"name\":\"id_bucket\"," +
        "\"transform\":\"bucket[8]\"," +
        "\"source-id\":1" +
        "},{" +
        "\"name\": \"data_bucket\"," +
        "\"transform\":\"bucket[16]\"," +
        "\"source-id\":2" +
        "}]" +
        "}";

    UnboundPartitionSpec actualSpec = PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String json = String.format("{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    PartitionSpec expectedSpec = PartitionSpec.builderFor(ID_DATA_SCHEMA)
        .bucket("id", 8)
        .bucket("data", 16)
        .withSpecId(1)
        .build();
    MetadataUpdate expected = new MetadataUpdate.AddPartitionSpec(expectedSpec.toUnbound());
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddPartitionSpecToJson() {
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString = "{" +
        "\"spec-id\":1," +
        "\"fields\":[{" +
        "\"name\":\"id_bucket\"," +
        "\"transform\":\"bucket[8]\"," +
        "\"source-id\":1," +
        "\"field-id\":1000" +
        "},{" +
        "\"name\":\"data_bucket\"," +
        "\"transform\":\"bucket[16]\"," +
        "\"source-id\":2," +
        "\"field-id\":1001" +
        "}]" +
        "}";

    UnboundPartitionSpec actualSpec = PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String expected = String.format("{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    // Partition spec order declaration needs to match declaration in spec string to be assigned the same field ids.
    PartitionSpec expectedSpec = PartitionSpec.builderFor(ID_DATA_SCHEMA)
        .bucket("id", 8)
        .bucket("data", 16)
        .withSpecId(1)
        .build();
    MetadataUpdate update = new MetadataUpdate.AddPartitionSpec(expectedSpec);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Add partition spec should convert to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetDefaultPartitionSpecToJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String expected = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate update = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Set default partition spec should serialize to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetDefaultPartitionSpecFromJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String json = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate.SetDefaultPartitionSpec expected = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddSortOrderToJson() {
    String action = MetadataUpdateParser.ADD_SORT_ORDER;
    UnboundSortOrder sortOrder = SortOrder.builderFor(ID_DATA_SCHEMA)
        .withOrderId(3)
        .asc("id", NullOrder.NULLS_FIRST)
        .desc("data")
        .build()
        .toUnbound();

    String expected = String.format("{\"action\":\"%s\",\"sort-order\":%s}", action, SortOrderParser.toJson(sortOrder));
    MetadataUpdate update = new MetadataUpdate.AddSortOrder(sortOrder);
    Assert.assertEquals("Add sort order should serialize to the correct JSON value",
        expected, MetadataUpdateParser.toJson(update));
  }

  @Test
  public void testAddSortOrderFromJson() {
    String action = MetadataUpdateParser.ADD_SORT_ORDER;
    UnboundSortOrder sortOrder = SortOrder.builderFor(ID_DATA_SCHEMA)
        .withOrderId(3)
        .asc("id", NullOrder.NULLS_FIRST)
        .desc("data")
        .build()
        .toUnbound();

    String json = String.format("{\"action\":\"%s\",\"sort-order\":%s}", action, SortOrderParser.toJson(sortOrder));
    MetadataUpdate.AddSortOrder expected = new MetadataUpdate.AddSortOrder(sortOrder);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }


  public void assertEquals(String action, MetadataUpdate expectedUpdate, MetadataUpdate actualUpdate) {
    switch (action) {
      case MetadataUpdateParser.ASSIGN_UUID:
        Assert.fail(String.format("MetadataUpdateParser for %s is not implemented", action));
        break;
      case MetadataUpdateParser.UPGRADE_FORMAT_VERSION:
        assertEqualsUpgradeFormatVersion((MetadataUpdate.UpgradeFormatVersion) expectedUpdate,
            (MetadataUpdate.UpgradeFormatVersion) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_SCHEMA:
        assertEqualsAddSchema((MetadataUpdate.AddSchema) expectedUpdate, (MetadataUpdate.AddSchema) actualUpdate);
        break;
      case MetadataUpdateParser.SET_CURRENT_SCHEMA:
        assertEqualsSetCurrentSchema((MetadataUpdate.SetCurrentSchema) expectedUpdate,
            (MetadataUpdate.SetCurrentSchema) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_PARTITION_SPEC:
        assertEqualsAddPartitionSpec((MetadataUpdate.AddPartitionSpec) expectedUpdate,
            (MetadataUpdate.AddPartitionSpec) actualUpdate);
        break;
      case MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC:
        assertEqualsSetDefaultPartitionSpec((MetadataUpdate.SetDefaultPartitionSpec) expectedUpdate,
            (MetadataUpdate.SetDefaultPartitionSpec) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_SORT_ORDER:
        assertEqualsAddSortOrder((MetadataUpdate.AddSortOrder) expectedUpdate,
            (MetadataUpdate.AddSortOrder) actualUpdate);
        break;
      case MetadataUpdateParser.SET_DEFAULT_SORT_ORDER:
      case MetadataUpdateParser.ADD_SNAPSHOT:
      case MetadataUpdateParser.REMOVE_SNAPSHOTS:
      case MetadataUpdateParser.SET_SNAPSHOT_REF:
      case MetadataUpdateParser.SET_PROPERTIES:
      case MetadataUpdateParser.REMOVE_PROPERTIES:
      case MetadataUpdateParser.SET_LOCATION:
        Assert.fail(String.format("MetadataUpdateParser for %s is not implemented yet", action));
        break;
      default:
        Assert.fail("Unrecognized metadata update action: " + action);
    }
  }

  private static void assertEqualsUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion expected, MetadataUpdate.UpgradeFormatVersion actual) {
    Assert.assertEquals("Format version should be equal", expected.formatVersion(), actual.formatVersion());
  }

  private static void assertEqualsAddSchema(MetadataUpdate.AddSchema expected, MetadataUpdate.AddSchema actual) {
    Assert.assertTrue("Schemas should be the same", expected.schema().sameSchema(actual.schema()));
    Assert.assertEquals("Last column id should be equal", expected.lastColumnId(), actual.lastColumnId());
  }

  private static void assertEqualsSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema expected, MetadataUpdate.SetCurrentSchema actual) {
    Assert.assertEquals("Schema id should be equal", expected.schemaId(), actual.schemaId());
  }

  private static void assertEqualsSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec expected, MetadataUpdate.SetDefaultPartitionSpec actual) {
    Assertions.assertThat(actual.specId()).isEqualTo(expected.specId());
  }

  private static void assertEqualsAddPartitionSpec(
      MetadataUpdate.AddPartitionSpec expected, MetadataUpdate.AddPartitionSpec actual) {
    Assert.assertEquals("Unbound partition specs should have the same spec id",
        expected.spec().specId(), actual.spec().specId());
    Assert.assertEquals("Unbound partition specs should have the same number of fields",
        expected.spec().fields().size(), actual.spec().fields().size());

    IntStream.range(0, expected.spec().fields().size())
        .forEachOrdered(i -> {
          UnboundPartitionSpec.UnboundPartitionField expectedField = expected.spec().fields().get(i);
          UnboundPartitionSpec.UnboundPartitionField actualField = actual.spec().fields().get(i);
          Assert.assertTrue(
              "Fields of the unbound partition spec should be the same",
              Objects.equals(expectedField.partitionId(), actualField.partitionId()) &&
                  expectedField.name().equals(actualField.name()) &&
                  Objects.equals(expectedField.transformAsString(), actualField.transformAsString()) &&
                  expectedField.sourceId() == actualField.sourceId());
        });
  }

  private static void assertEqualsAddSortOrder(
      MetadataUpdate.AddSortOrder expected, MetadataUpdate.AddSortOrder actual) {
    UnboundSortOrder expectedSortOrder = expected.sortOrder();
    UnboundSortOrder actualSortOrder = actual.sortOrder();
    Assert.assertEquals("Order id of the sort order should be the same",
        expected.sortOrder().orderId(), actual.sortOrder().orderId());

    Assert.assertEquals("Sort orders should have the same number of fields",
        expected.sortOrder().fields().size(), actual.sortOrder().fields().size());

    IntStream.range(0, expected.sortOrder().fields().size())
        .forEachOrdered(i -> {
          UnboundSortOrder.UnboundSortField expectedField = expected.sortOrder().fields().get(i);
          UnboundSortOrder.UnboundSortField actualField = actual.sortOrder().fields().get(i);
          Assert.assertTrue("Fields of the sort order should be the same",
              expectedField.sourceId() == actualField.sourceId() &&
              expectedField.nullOrder().equals(actualField.nullOrder()) &&
              expectedField.direction().equals(actualField.direction()) &&
              Objects.equals(expectedField.transformAsString(), actualField.transformAsString()));
        });
  }
}

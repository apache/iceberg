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
package org.apache.iceberg.rest.requests;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestCreateTableRequest extends RequestResponseTestBase<CreateTableRequest> {

  /* Values used to fill in request fields */
  private static final Map<String, String> SAMPLE_PROPERTIES = ImmutableMap.of("owner", "Hank");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();
  private static final String SAMPLE_NAME = "test_tbl";
  private static final String SAMPLE_LOCATION = "file://tmp/location/";
  private static final Schema SAMPLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final String SAMPLE_SCHEMA_JSON = SchemaParser.toJson(SAMPLE_SCHEMA);
  private static final PartitionSpec SAMPLE_SPEC =
      PartitionSpec.builderFor(SAMPLE_SCHEMA).bucket("id", 16).build();
  private static final SortOrder SAMPLE_WRITE_ORDER =
      SortOrder.builderFor(SAMPLE_SCHEMA).asc("data", NullOrder.NULLS_LAST).build();

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJsonRaw =
        "{\"name\":\"test_tbl\",\"location\":\"file://tmp/location/\",\"schema\":{\"type\":\"struct\","
            + "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"int\"},"
            + "{\"id\":2,\"name\":\"data\",\"required\":false,\"type\":\"string\"}]},\"partition-spec\":{\"spec-id\":0,"
            + "\"fields\":[{\"name\":\"id_bucket\",\"transform\":\"bucket[16]\",\"source-id\":1,\"field-id\":1000}]},"
            + "\"write-order\":{\"order-id\":1,\"fields\":"
            + "[{\"transform\":\"identity\",\"source-id\":2,\"direction\":\"asc\",\"null-order\":\"nulls-last\"}]},"
            + "\"properties\":{\"owner\":\"Hank\"},\"stage-create\":false}";

    CreateTableRequest req =
        CreateTableRequest.builder()
            .withName(SAMPLE_NAME)
            .withLocation(SAMPLE_LOCATION)
            .withSchema(SAMPLE_SCHEMA)
            .setProperties(SAMPLE_PROPERTIES)
            .withPartitionSpec(SAMPLE_SPEC)
            .withWriteOrder(SAMPLE_WRITE_ORDER)
            .build();

    assertRoundTripSerializesEquallyFrom(fullJsonRaw, req);

    // The same JSON but using existing parsers for clarity and staging the request instead of
    // committing
    String jsonStagedReq =
        String.format(
            "{\"name\":\"%s\",\"location\":\"%s\",\"schema\":%s,\"partition-spec\":%s,"
                + "\"write-order\":%s,\"properties\":%s,\"stage-create\":%b}",
            SAMPLE_NAME,
            SAMPLE_LOCATION,
            SchemaParser.toJson(SAMPLE_SCHEMA),
            PartitionSpecParser.toJson(SAMPLE_SPEC.toUnbound()),
            SortOrderParser.toJson(SAMPLE_WRITE_ORDER.toUnbound()),
            mapper().writeValueAsString(SAMPLE_PROPERTIES),
            true);

    CreateTableRequest stagedReq =
        CreateTableRequest.builder()
            .withName(SAMPLE_NAME)
            .withLocation(SAMPLE_LOCATION)
            .withSchema(SAMPLE_SCHEMA)
            .setProperty("owner", "Hank")
            .withPartitionSpec(SAMPLE_SPEC)
            .withWriteOrder(SAMPLE_WRITE_ORDER)
            .stageCreate()
            .build();

    assertRoundTripSerializesEquallyFrom(jsonStagedReq, stagedReq);

    // Partition spec and write order can be null or use PartitionSpec.unpartitioned() and
    // SortOrder.unsorted()
    String jsonWithExplicitUnsortedUnordered =
        String.format(
            "{\"name\":\"%s\",\"location\":null,\"schema\":%s,\"partition-spec\":%s,"
                + "\"write-order\":%s,\"properties\":{},\"stage-create\":%b}",
            SAMPLE_NAME,
            SchemaParser.toJson(SAMPLE_SCHEMA),
            PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
            SortOrderParser.toJson(SortOrder.unsorted().toUnbound()),
            /* stageCreate */ false);

    CreateTableRequest reqOnlyRequiredFieldsExplicitDefaults =
        CreateTableRequest.builder()
            .withName(SAMPLE_NAME)
            .withLocation(null)
            .withSchema(SAMPLE_SCHEMA)
            .setProperties(EMPTY_PROPERTIES)
            .withPartitionSpec(PartitionSpec.unpartitioned())
            .withWriteOrder(SortOrder.unsorted())
            .build();

    assertRoundTripSerializesEquallyFrom(
        jsonWithExplicitUnsortedUnordered, reqOnlyRequiredFieldsExplicitDefaults);

    String jsonOnlyRequiredFieldsNullAsDefault =
        String.format(
            "{\"name\":\"%s\",\"location\":null,\"schema\":%s,\"partition-spec\":null,\"write-order\":null,"
                + "\"properties\":{},\"stage-create\":false}",
            SAMPLE_NAME, SchemaParser.toJson(SAMPLE_SCHEMA));

    CreateTableRequest reqOnlyRequiredFieldsMissingDefaults =
        CreateTableRequest.builder()
            .withName(SAMPLE_NAME)
            .withSchema(SAMPLE_SCHEMA)
            .withPartitionSpec(null)
            .withWriteOrder(null)
            .build();

    assertRoundTripSerializesEquallyFrom(
        jsonOnlyRequiredFieldsNullAsDefault, reqOnlyRequiredFieldsMissingDefaults);
  }

  @Test
  // Test cases that can't be constructed with our Builder class but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    // Name and schema are only two required fields
    String jsonOnlyRequiredFieldsMissingDefaults =
        String.format(
            "{\"name\":\"%s\",\"schema\":%s}", SAMPLE_NAME, SchemaParser.toJson(SAMPLE_SCHEMA));

    CreateTableRequest reqOnlyRequiredFieldsMissingDefaults =
        CreateTableRequest.builder().withName(SAMPLE_NAME).withSchema(SAMPLE_SCHEMA).build();

    assertEquals(
        deserialize(jsonOnlyRequiredFieldsMissingDefaults), reqOnlyRequiredFieldsMissingDefaults);
  }

  @Test
  public void testDeserializeInvalidRequest() {
    String jsonMissingSchema =
        "{\"name\":\"foo\",\"location\":null,\"partition-spec\":null,\"write-order\":null,\"properties\":{},"
            + "\"stage-create\":false}";
    AssertHelpers.assertThrows(
        "A JSON request with the keys spelled incorrectly should fail to deserialize and validate",
        IllegalArgumentException.class,
        "Invalid schema: null",
        () -> deserialize(jsonMissingSchema));

    String jsonMissingName =
        String.format(
            "{\"location\":null,\"schema\":%s,\"spec\":null,\"write-order\":null,\"properties\":{},"
                + "\"stage-create\":false}",
            SAMPLE_SCHEMA_JSON);
    AssertHelpers.assertThrows(
        "A JSON request with the keys spelled incorrectly should fail to deserialize and validate",
        IllegalArgumentException.class,
        "Invalid table name: null",
        () -> deserialize(jsonMissingName));

    String jsonIncorrectTypeForProperties =
        String.format(
            "{\"name\":\"foo\",\"location\":null,\"schema\":%s,\"partition-spec\":null,\"write-order\":null,"
                + "\"properties\":[],\"stage-create\":false}",
            SAMPLE_SCHEMA_JSON);
    AssertHelpers.assertThrows(
        "A JSON request with incorrect types for fields should fail to parse and validate",
        JsonProcessingException.class,
        () -> deserialize(jsonIncorrectTypeForProperties));

    AssertHelpers.assertThrows(
        "An empty JSON object should not parse into a CreateNamespaceRequest instance that passes validation",
        IllegalArgumentException.class,
        "Invalid table name: null",
        () -> deserialize("{}"));

    AssertHelpers.assertThrows(
        "An empty JSON request should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null for the namespace",
        NullPointerException.class,
        "Invalid name: null",
        () -> CreateTableRequest.builder().withName(null));

    AssertHelpers.assertThrows(
        "The builder should not allow using null for the schema",
        NullPointerException.class,
        "Invalid schema: null",
        () -> CreateTableRequest.builder().withSchema(null));

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null collection of properties",
        NullPointerException.class,
        "Invalid collection of properties: null",
        () -> CreateTableRequest.builder().setProperties(null));

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the properties to set",
        IllegalArgumentException.class,
        "Invalid property: null",
        () -> CreateTableRequest.builder().setProperties(mapWithNullKey));

    Map<String, String> mapWithNullValue = Maps.newHashMap();
    mapWithNullValue.put("a", null);
    mapWithNullValue.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value in the properties to set",
        IllegalArgumentException.class,
        "Invalid value for properties [a]: null",
        () -> CreateTableRequest.builder().setProperties(mapWithNullValue).build());

    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value when setting a single property",
        IllegalArgumentException.class,
        "Invalid value for property foo: null",
        () -> CreateTableRequest.builder().setProperty("foo", null));

    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key when setting a single property",
        IllegalArgumentException.class,
        "Invalid property: null",
        () -> CreateTableRequest.builder().setProperty(null, "foo"));
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {
      "name", "location", "schema", "partition-spec", "write-order", "stage-create", "properties"
    };
  }

  @Override
  public CreateTableRequest createExampleInstance() {
    return CreateTableRequest.builder()
        .withName(SAMPLE_NAME)
        .withLocation(SAMPLE_LOCATION)
        .withSchema(SAMPLE_SCHEMA)
        .withPartitionSpec(SAMPLE_SPEC)
        .withWriteOrder(SAMPLE_WRITE_ORDER)
        .setProperties(SAMPLE_PROPERTIES)
        .stageCreate()
        .build();
  }

  @Override
  public void assertEquals(CreateTableRequest actual, CreateTableRequest expected) {
    Assert.assertEquals("Name should be the same", expected.name(), actual.name());
    Assert.assertEquals(
        "Location should be the same if provided", expected.location(), actual.location());
    Assert.assertTrue(
        "Schemas should be equivalent and have same schema id",
        expected.schema().sameSchema(actual.schema())
            && expected.schema().schemaId() == actual.schema().schemaId());
    Assert.assertEquals("Partition spec should be equal", expected.spec(), actual.spec());
    Assert.assertEquals(
        "Write [sort] order should be the same", expected.writeOrder(), actual.writeOrder());
    Assert.assertEquals(
        "Properties should be the same", expected.properties(), actual.properties());
    Assert.assertEquals(
        "Stage create should be equal", expected.stageCreate(), actual.stageCreate());
  }

  @Override
  public CreateTableRequest deserialize(String json) throws JsonProcessingException {
    CreateTableRequest request = mapper().readValue(json, CreateTableRequest.class);
    request.validate();
    return request;
  }
}

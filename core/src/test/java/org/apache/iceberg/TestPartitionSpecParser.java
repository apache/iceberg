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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionSpecParser extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @TestTemplate
  public void testToJsonForV1Table() {
    String expected =
        "{\n"
            + "  \"spec-id\" : 0,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"field-id\" : 1000\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(table.spec(), true)).isEqualTo(expected);

    PartitionSpec spec =
        PartitionSpec.builderFor(table.schema()).bucket("id", 8).bucket("data", 16).build();

    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    expected =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 1,\n"
            + "    \"field-id\" : 1000\n"
            + "  }, {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(table.spec(), true)).isEqualTo(expected);
  }

  @TestTemplate
  public void testToJsonWithMultipleSourceIds() {
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Arrays.asList(1, 2), 1001, "id_data")
            .build();

    String expected =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_data\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(spec, true)).isEqualTo(expected);
  }

  @TestTemplate
  public void testToJsonWithMultipleFieldsAndMultipleSourceIds() {
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Arrays.asList(1, 2), 1001, "id_data_bucket")
            .addField("truncate[10]", Arrays.asList(3, 7, 10), 1002, "multi_truncate")
            .build();

    String expected =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_data_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  }, {\n"
            + "    \"name\" : \"multi_truncate\",\n"
            + "    \"transform\" : \"truncate[10]\",\n"
            + "    \"source-ids\" : [ 3, 7, 10 ],\n"
            + "    \"field-id\" : 1002\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(spec, true)).isEqualTo(expected);
  }

  @TestTemplate
  public void testToJsonWithSingleSourceId() {
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(0)
            .addField("bucket[16]", Collections.singletonList(2), 1000, "data_bucket")
            .build();

    String expected =
        "{\n"
            + "  \"spec-id\" : 0,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"field-id\" : 1000\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(spec, true)).isEqualTo(expected);
  }

  @TestTemplate
  public void testFromJsonWithFieldId() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 1,\n"
            + "    \"field-id\" : 1001\n"
            + "  }, {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"field-id\" : 1000\n"
            + "  } ]\n"
            + "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(table.schema(), specString);

    assertThat(spec.fields()).hasSize(2);
    // should be the field ids in the JSON
    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1001);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1000);
  }

  @TestTemplate
  public void testFromJsonWithoutFieldId() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 1\n"
            + "  }, {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2\n"
            + "  } ]\n"
            + "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(table.schema(), specString);

    assertThat(spec.fields()).hasSize(2);
    // should be the default assignment
    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1000);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testFromJsonWithSourceIds() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 1,\n"
            + "    \"source-ids\" : [ 1 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  }, {\n"
            + "    \"name\" : \"data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"source-ids\" : [ 2 ],\n"
            + "    \"field-id\" : 1000\n"
            + "  } ]\n"
            + "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(table.schema(), specString);

    assertThat(spec.fields()).hasSize(2);
    assertThat(spec.fields().get(0).sourceId()).isEqualTo(1);
    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1001);
    assertThat(spec.fields().get(1).sourceId()).isEqualTo(2);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1000);
  }

  @TestTemplate
  public void testFromJsonWithSourceIdsOnly() {
    // source-ids without source-id for forward compatibility
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ 1 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(table.schema(), specString);

    assertThat(spec.fields()).hasSize(1);
    assertThat(spec.fields().get(0).sourceId()).isEqualTo(1);
    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testFromJsonWithMultipleSourceIds() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_data\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";

    UnboundPartitionSpec unboundSpec = JsonUtil.parse(specString, PartitionSpecParser::fromJson);

    assertThat(unboundSpec.fields()).hasSize(1);
    assertThat(unboundSpec.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(unboundSpec.fields().get(0).sourceId()).isEqualTo(1);
    assertThat(unboundSpec.fields().get(0).partitionId()).isEqualTo(1001);

    // round-trip should preserve source-ids
    String json = PartitionSpecParser.toJson(unboundSpec, true);
    assertThat(json).contains("\"source-ids\" : [ 1, 2 ]");
    assertThat(json).doesNotContain("\"source-id\"");

    UnboundPartitionSpec roundTripped = JsonUtil.parse(json, PartitionSpecParser::fromJson);
    assertThat(roundTripped.fields().get(0).sourceIds()).containsExactly(1, 2);
  }

  @TestTemplate
  public void testFromJsonWithEmptySourceIds() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ ],\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";

    assertThatThrownBy(() -> PartitionSpecParser.fromJson(table.schema(), specString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("source-ids must be a non-empty array");
  }

  @TestTemplate
  public void testFromJsonWithSourceIdsNotAnArray() {
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : 1,\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";

    assertThatThrownBy(() -> PartitionSpecParser.fromJson(table.schema(), specString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("source-ids must be a non-empty array");
  }

  @TestTemplate
  public void testFromJsonWithMultipleSourceIdsWithoutFieldId() {
    // source-ids without field-id should auto-increment field ids
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_data\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-ids\" : [ 1, 2 ]\n"
            + "  } ]\n"
            + "}";

    UnboundPartitionSpec unboundSpec = JsonUtil.parse(specString, PartitionSpecParser::fromJson);

    assertThat(unboundSpec.fields()).hasSize(1);
    assertThat(unboundSpec.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(unboundSpec.fields().get(0).partitionId()).isNull();
  }

  @TestTemplate
  public void testFromJsonSourceIdsPrecedenceOverSourceId() {
    // when both source-id and source-ids are present, source-ids takes precedence
    String specString =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_data\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 99,\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"field-id\" : 1001\n"
            + "  } ]\n"
            + "}";

    UnboundPartitionSpec unboundSpec = JsonUtil.parse(specString, PartitionSpecParser::fromJson);

    assertThat(unboundSpec.fields()).hasSize(1);
    // source-ids should be used, not source-id
    assertThat(unboundSpec.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(unboundSpec.fields().get(0).sourceId()).isEqualTo(1);
  }

  @TestTemplate
  public void testToJsonCompactWithMultipleSourceIds() {
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Arrays.asList(1, 2), 1001, "id_data")
            .build();

    String json = PartitionSpecParser.toJson(spec, false);
    assertThat(json).contains("\"source-ids\":[1,2]");
    assertThat(json).doesNotContain("\"source-id\"");
  }

  @TestTemplate
  public void testToJsonMixedSingleAndMultipleSourceIds() {
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Collections.singletonList(1), 1001, "id_bucket")
            .addField("bucket[16]", Arrays.asList(1, 2), 1002, "id_data_bucket")
            .build();

    String expected =
        "{\n"
            + "  \"spec-id\" : 1,\n"
            + "  \"fields\" : [ {\n"
            + "    \"name\" : \"id_bucket\",\n"
            + "    \"transform\" : \"bucket[8]\",\n"
            + "    \"source-id\" : 1,\n"
            + "    \"field-id\" : 1001\n"
            + "  }, {\n"
            + "    \"name\" : \"id_data_bucket\",\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"field-id\" : 1002\n"
            + "  } ]\n"
            + "}";
    assertThat(PartitionSpecParser.toJson(spec, true)).isEqualTo(expected);
  }

  @TestTemplate
  public void testRoundTripSingleSourceIdUsesSingularKey() {
    // a single-element source-ids list should serialize as source-id (not source-ids)
    UnboundPartitionSpec spec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Collections.singletonList(1), 1001, "id_bucket")
            .build();

    String json = PartitionSpecParser.toJson(spec, true);
    assertThat(json).contains("\"source-id\" : 1");
    assertThat(json).doesNotContain("\"source-ids\"");

    UnboundPartitionSpec parsed = JsonUtil.parse(json, PartitionSpecParser::fromJson);
    assertThat(parsed.fields().get(0).sourceIds()).containsExactly(1);
  }

  @TestTemplate
  public void testFromJsonFieldsWithMultipleSourceIds() {
    // exercise the fromJsonFields path used by table metadata parsing
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    String fieldsJson =
        "[{\"name\":\"id_data\",\"transform\":\"bucket[8]\","
            + "\"source-ids\":[1,2],\"field-id\":1001}]";

    PartitionSpec spec =
        JsonUtil.parse(fieldsJson, node -> PartitionSpecParser.fromJsonFields(schema, 1, node));
    assertThat(spec.specId()).isEqualTo(1);
    assertThat(spec.fields()).hasSize(1);
    assertThat(spec.fields().get(0).sourceId()).isEqualTo(1);
    assertThat(spec.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testBoundSpecRoundTripPreservesSourceIds() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    // build an unbound spec with multi-arg source-ids, bind it, then round-trip through JSON
    UnboundPartitionSpec unboundSpec =
        UnboundPartitionSpec.builder()
            .withSpecId(1)
            .addField("bucket[8]", Arrays.asList(1, 2), 1001, "id_data_bucket")
            .build();

    PartitionSpec boundSpec = unboundSpec.bind(schema);
    assertThat(boundSpec.fields().get(0).sourceIds()).containsExactly(1, 2);

    // round-trip through JSON via toUnbound
    String json = PartitionSpecParser.toJson(boundSpec, true);
    assertThat(json).contains("\"source-ids\" : [ 1, 2 ]");

    UnboundPartitionSpec parsed = JsonUtil.parse(json, PartitionSpecParser::fromJson);
    assertThat(parsed.fields().get(0).sourceIds()).containsExactly(1, 2);
  }

  @TestTemplate
  public void testTransforms() {
    for (PartitionSpec spec : PartitionSpecTestBase.SPECS) {
      assertThat(roundTripJSON(spec)).isEqualTo(spec);
    }
  }

  @TestTemplate
  public void testTransformRoundTripWithMultipleSourceIds() {
    // test various transform types with multiple source-ids
    String[] transforms = {"bucket[16]", "identity", "truncate[10]"};

    for (String transform : transforms) {
      UnboundPartitionSpec spec =
          UnboundPartitionSpec.builder()
              .withSpecId(1)
              .addField(transform, Arrays.asList(1, 2, 7), 1001, "multi_field")
              .build();

      String json = PartitionSpecParser.toJson(spec, true);
      assertThat(json).contains("\"source-ids\" : [ 1, 2, 7 ]");
      assertThat(json).doesNotContain("\"source-id\"");
      assertThat(json).contains("\"transform\" : \"" + transform + "\"");

      UnboundPartitionSpec parsed = JsonUtil.parse(json, PartitionSpecParser::fromJson);
      assertThat(parsed.specId()).isEqualTo(1);
      assertThat(parsed.fields()).hasSize(1);
      assertThat(parsed.fields().get(0).sourceIds()).containsExactly(1, 2, 7);
      assertThat(parsed.fields().get(0).sourceId()).isEqualTo(1);
      assertThat(parsed.fields().get(0).transformAsString()).isEqualTo(transform);
      assertThat(parsed.fields().get(0).partitionId()).isEqualTo(1001);
      assertThat(parsed.fields().get(0).name()).isEqualTo("multi_field");
    }
  }

  private static PartitionSpec roundTripJSON(PartitionSpec spec) {
    return PartitionSpecParser.fromJson(
        PartitionSpecTestBase.SCHEMA, PartitionSpecParser.toJson(spec));
  }
}

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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionSpecParser extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testTransforms() {
    for (PartitionSpec spec : PartitionSpecTestBase.SPECS) {
      assertThat(roundTripJSON(spec)).isEqualTo(spec);
    }
  }

  private static PartitionSpec roundTripJSON(PartitionSpec spec) {
    return PartitionSpecParser.fromJson(
        PartitionSpecTestBase.SCHEMA, PartitionSpecParser.toJson(spec));
  }
}

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

import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPartitionSpecParser {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
  );

  @Test
  public void testToJson() {
    String expected = "{\n" +
        "  \"spec-id\" : 0,\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"data_bucket\",\n" +
        "    \"transform\" : \"bucket[16]\",\n" +
        "    \"source-id\" : 2,\n" +
        "    \"field-id\" : 1000\n" +
        "  } ]\n" +
        "}";
    PartitionSpec initialSpec = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
    Assert.assertEquals(expected, PartitionSpecParser.toJson(initialSpec, true));

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(SCHEMA)
        .withSpecId(1)
        .alwaysNull("data", "1000__[removed]")
        .bucket("id", 8)
        .bucket("data", 4)
        .build();

    expected = "{\n" +
        "  \"spec-id\" : 1,\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"1000__[removed]\",\n" +
        "    \"transform\" : \"void\",\n" +
        "    \"source-id\" : 2,\n" +
        "    \"field-id\" : 1000\n" +
        "  }, {\n" +
        "    \"name\" : \"id_bucket\",\n" +
        "    \"transform\" : \"bucket[8]\",\n" +
        "    \"source-id\" : 1,\n" +
        "    \"field-id\" : 1001\n" +
        "  }, {\n" +
        "    \"name\" : \"data_bucket\",\n" +
        "    \"transform\" : \"bucket[4]\",\n" +
        "    \"source-id\" : 2,\n" +
        "    \"field-id\" : 1002\n" +
        "  } ]\n" +
        "}";
    Assert.assertEquals(expected, PartitionSpecParser.toJson(evolvedSpec, true));
  }

  @Test
  public void testFromJsonWithFieldId() {
    String specString = "{\n" +
        "  \"spec-id\" : 1,\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"id_bucket\",\n" +
        "    \"transform\" : \"bucket[8]\",\n" +
        "    \"source-id\" : 1,\n" +
        "    \"field-id\" : 1001\n" +
        "  }, {\n" +
        "    \"name\" : \"data_bucket\",\n" +
        "    \"transform\" : \"bucket[16]\",\n" +
        "    \"source-id\" : 2,\n" +
        "    \"field-id\" : 1000\n" +
        "  } ]\n" +
        "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(SCHEMA, specString);

    Assert.assertEquals(2, spec.fields().size());
    // should be the field ids in the JSON
    Assert.assertEquals(1001, spec.fields().get(0).fieldId());
    Assert.assertEquals(1000, spec.fields().get(1).fieldId());
  }

  @Test
  public void testFromJsonWithoutFieldId() {
    String specString = "{\n" +
        "  \"spec-id\" : 1,\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"id_bucket\",\n" +
        "    \"transform\" : \"bucket[8]\",\n" +
        "    \"source-id\" : 1\n" +
        "  }, {\n" +
        "    \"name\" : \"data_bucket\",\n" +
        "    \"transform\" : \"bucket[16]\",\n" +
        "    \"source-id\" : 2\n" +
        "  } ]\n" +
        "}";

    PartitionSpec spec = PartitionSpecParser.fromJson(SCHEMA, specString);

    Assert.assertEquals(2, spec.fields().size());
    // should be the default assignment
    Assert.assertEquals(1000, spec.fields().get(0).fieldId());
    Assert.assertEquals(1001, spec.fields().get(1).fieldId());
  }

  @Test
  public void testTransforms() {
    for (PartitionSpec spec : PartitionSpecTestBase.SPECS) {
      Assert.assertEquals("To/from JSON should produce equal partition spec",
          spec, roundTripJSON(spec));
    }
  }

  private static PartitionSpec roundTripJSON(PartitionSpec spec) {
    return PartitionSpecParser.fromJson(PartitionSpecTestBase.SCHEMA, PartitionSpecParser.toJson(spec));
  }
}

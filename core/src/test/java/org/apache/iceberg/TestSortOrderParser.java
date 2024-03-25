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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.DESC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.UnknownTransform;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSortOrderParser extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @TestTemplate
  public void testUnknownTransforms() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"custom_transform\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"direction\" : \"desc\",\n"
            + "    \"null-order\" : \"nulls-first\"\n"
            + "  } ]\n"
            + "}";

    SortOrder order = SortOrderParser.fromJson(table.schema(), jsonString);

    assertThat(order.orderId()).isEqualTo(10);
    assertThat(order.fields()).hasSize(1);
    assertThat(order.fields().get(0).transform())
        .isInstanceOf(UnknownTransform.class)
        .asString()
        .isEqualTo("custom_transform");
    assertThat(order.fields().get(0).sourceId()).isEqualTo(2);
    assertThat(order.fields().get(0).direction()).isEqualTo(DESC);
    assertThat(order.fields().get(0).nullOrder()).isEqualTo(NULLS_FIRST);
  }

  @TestTemplate
  public void invalidSortDirection() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"custom_transform\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"direction\" : \"invalid\",\n"
            + "    \"null-order\" : \"nulls-first\"\n"
            + "  } ]\n"
            + "}";

    assertThatThrownBy(() -> SortOrderParser.fromJson(table.schema(), jsonString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid sort direction: invalid");
  }
}

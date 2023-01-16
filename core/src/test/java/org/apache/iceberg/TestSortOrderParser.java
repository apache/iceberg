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

import org.apache.iceberg.transforms.UnknownTransform;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestSortOrderParser extends TableTestBase {
  public TestSortOrderParser() {
    super(1);
  }

  @Test
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

    Assert.assertEquals(10, order.orderId());
    Assert.assertEquals(1, order.fields().size());
    Assertions.assertThat(order.fields().get(0).transform()).isInstanceOf(UnknownTransform.class);
    Assert.assertEquals("custom_transform", order.fields().get(0).transform().toString());
    Assert.assertEquals(2, order.fields().get(0).sourceId());
    Assert.assertEquals(DESC, order.fields().get(0).direction());
    Assert.assertEquals(NULLS_FIRST, order.fields().get(0).nullOrder());
  }

  @Test
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

    Assertions.assertThatThrownBy(() -> SortOrderParser.fromJson(table.schema(), jsonString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid sort direction: invalid");
  }
}

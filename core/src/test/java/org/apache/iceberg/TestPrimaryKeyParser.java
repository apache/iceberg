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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestPrimaryKeyParser extends TableTestBase {

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        {1},
        {2}
    };
  }

  public TestPrimaryKeyParser(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testToJson() {
    String expected = "{\"key-id\":0,\"enforce-uniqueness\":false,\"fields\":[]}";
    Assert.assertEquals(expected, PrimaryKeyParser.toJson(table.primaryKey(), false));

    PrimaryKey key = PrimaryKey.builderFor(table.schema())
        .addField("id")
        .addField("data")
        .withEnforceUniqueness(true)
        .build();

    table.ops().commit(table.ops().current(), table.ops().current().updatePrimaryKey(key));

    expected = "{\n" +
        "  \"key-id\" : 1,\n" +
        "  \"enforce-uniqueness\" : true,\n" +
        "  \"fields\" : [ {\n" +
        "    \"source-id\" : 1\n" +
        "  }, {\n" +
        "    \"source-id\" : 2\n" +
        "  } ]\n" +
        "}";
    Assert.assertEquals(expected, PrimaryKeyParser.toJson(key, true));
  }

  @Test
  public void testFromJson() {
    String keyString = "{\n" +
        "  \"key-id\" : 1,\n" +
        "  \"enforce-uniqueness\" : true,\n" +
        "  \"fields\" : [ {\n" +
        "    \"source-id\" : 1\n" +
        "  } ]\n" +
        "}";

    PrimaryKey expectedKey = PrimaryKey.builderFor(table.schema())
        .addField("id")
        .withEnforceUniqueness(true)
        .build();

    PrimaryKey key = PrimaryKeyParser.fromJson(table.schema(), keyString);
    Assert.assertEquals(expectedKey, key);
  }
}

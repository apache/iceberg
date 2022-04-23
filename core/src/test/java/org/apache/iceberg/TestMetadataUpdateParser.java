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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataUpdateParser {

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
    String action = "upgrade-format-version";
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

  public void assertEquals(String action, MetadataUpdate expectedUpdate, MetadataUpdate actualUpdate) {
    switch (action) {
      case "upgrade-format-version":
        MetadataUpdate.UpgradeFormatVersion expected = (MetadataUpdate.UpgradeFormatVersion) expectedUpdate;
        MetadataUpdate.UpgradeFormatVersion actual = (MetadataUpdate.UpgradeFormatVersion) actualUpdate;
        Assert.assertEquals("Format version should be equal", expected.formatVersion(), actual.formatVersion());
        break;
      default:
        Assert.fail("Unrecognized metadata update action: " + action);
    }
  }
}

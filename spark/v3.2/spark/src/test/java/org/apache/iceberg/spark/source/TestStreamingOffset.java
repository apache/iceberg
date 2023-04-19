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
package org.apache.iceberg.spark.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestStreamingOffset {

  @Test
  public void testJsonConversion() {
    StreamingOffset[] expected =
        new StreamingOffset[] {
          new StreamingOffset(System.currentTimeMillis(), 1L, false),
          new StreamingOffset(System.currentTimeMillis(), 2L, false),
          new StreamingOffset(System.currentTimeMillis(), 3L, false),
          new StreamingOffset(System.currentTimeMillis(), 4L, true)
        };
    Assert.assertArrayEquals(
        "StreamingOffsets should match",
        expected,
        Arrays.stream(expected).map(elem -> StreamingOffset.fromJson(elem.json())).toArray());
  }

  @Test
  public void testToJson() throws Exception {
    StreamingOffset expected = new StreamingOffset(System.currentTimeMillis(), 1L, false);
    ObjectNode actual = JsonUtil.mapper().createObjectNode();
    actual.put("version", 1);
    actual.put("snapshot_id", expected.snapshotId());
    actual.put("position", 1L);
    actual.put("scan_all_files", false);
    String expectedJson = expected.json();
    String actualJson = JsonUtil.mapper().writeValueAsString(actual);
    Assert.assertEquals("Json should match", expectedJson, actualJson);
  }
}

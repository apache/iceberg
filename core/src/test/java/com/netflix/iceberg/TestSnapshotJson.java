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

package com.netflix.iceberg;

import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotJson {
  @Test
  public void testJsonConversion() {
    Snapshot expected = new BaseSnapshot(null, System.currentTimeMillis(),
        "file:/tmp/manifest1.avro", "file:/tmp/manifest2.avro");
    String json = SnapshotParser.toJson(expected);
    Snapshot snapshot = SnapshotParser.fromJson(null, json);

    Assert.assertEquals("Snapshot ID should match",
        expected.snapshotId(), snapshot.snapshotId());
    Assert.assertEquals("Files should match",
        expected.manifests(), snapshot.manifests());
  }
}

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

package org.apache.iceberg.events;

import org.apache.iceberg.util.EventParser;
import org.junit.Assert;
import org.junit.Test;

public class TestEventParser {
  @Test
  public void testCreateSnapshotEventSerialization() {
    String createSnapshotEventMessageBefore = "{\n" +
            "  \"event-type\" : \"org.apache.iceberg.events.CreateSnapshotEvent\",\n" +
            "  \"table-name\" : \"glue.default1.demo\",\n" +
            "  \"operation\" : \"append\",\n" +
            "  \"snapshot-id\" : 3898248035543915183,\n" +
            "  \"sequence-number\" : 0,\n" +
            "  \"summary\" : {\n" +
            "    \"spark.app.id\" : \"local-1645224366005\",\n" +
            "    \"added-data-files\" : \"3\",\n" +
            "    \"added-records\" : \"3\",\n" +
            "    \"added-files-size\" : \"1929\",\n" +
            "    \"changed-partition-count\" : \"1\",\n" +
            "    \"total-records\" : \"3\",\n" +
            "    \"total-files-size\" : \"1929\",\n" +
            "    \"total-data-files\" : \"3\",\n" +
            "    \"total-delete-files\" : \"0\",\n" +
            "    \"total-position-deletes\" : \"0\",\n" +
            "    \"total-equality-deletes\" : \"0\"\n" +
            "  }\n" +
            "}";

    Object createSnapShotEvent = EventParser.fromJson(createSnapshotEventMessageBefore);
    String createSnapshotEventMessageAfter = EventParser.toJson(createSnapShotEvent, true);
    Assert.assertEquals(createSnapshotEventMessageBefore, createSnapshotEventMessageAfter);
  }

  @Test
  public void testScanEventSerialization() {
    String scanEventMessageBefore = "{\n" +
            "  \"event-type\" : \"org.apache.iceberg.events.ScanEvent\",\n" +
            "  \"table-name\" : \"glue.default1.check\",\n" +
            "  \"snapshot-id\" : 4466314262163129668,\n" +
            "  \"expression\" : {\n" +
            "    \"type\" : \"unbounded-predicate\",\n" +
            "    \"operation\" : \"eq\",\n" +
            "    \"term\" : {\n" +
            "      \"type\" : \"named-reference\",\n" +
            "      \"value\" : \"id\"\n" +
            "    },\n" +
            "    \"literals\" : [ {\n" +
            "      \"type\" : \"int\",\n" +
            "      \"value\" : \"2\\u0000\\u0000\\u0000\"\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"projection\" : {\n" +
            "    \"type\" : \"struct\",\n" +
            "    \"schema-id\" : 0,\n" +
            "    \"fields\" : [ {\n" +
            "      \"id\" : 1,\n" +
            "      \"name\" : \"id\",\n" +
            "      \"required\" : true,\n" +
            "      \"type\" : \"int\"\n" +
            "    }, {\n" +
            "      \"id\" : 2,\n" +
            "      \"name\" : \"data\",\n" +
            "      \"required\" : false,\n" +
            "      \"type\" : \"string\"\n" +
            "    } ]\n" +
            "  }\n" +
            "}";

    Object scanEvent = EventParser.fromJson(scanEventMessageBefore);

    String scanEventMessageAfter = EventParser.toJson(scanEvent, true);
    Assert.assertEquals(scanEventMessageBefore, scanEventMessageAfter);
  }
}

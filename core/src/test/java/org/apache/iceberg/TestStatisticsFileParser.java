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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestStatisticsFileParser {

  private static final ImmutableList<BlobMetadata> BLOB_METADATA =
      ImmutableList.of(
          new GenericBlobMetadata(
              "some-stats", 42L, 3L, ImmutableList.of(1, 2), ImmutableMap.of("prop", "value")));

  @Test
  void roundTripSerdeWithKeyId() {
    StatisticsFile statisticsFile =
        new GenericStatisticsFile(
            42L, "/stats/file.puffin", 124L, 27L, "some-key-id", BLOB_METADATA);

    String json = StatisticsFileParser.toJson(statisticsFile);
    assertThat(json)
        .isEqualTo(
            "{\"snapshot-id\":42,\"statistics-path\":\"/stats/file.puffin\","
                + "\"file-size-in-bytes\":124,\"file-footer-size-in-bytes\":27,"
                + "\"key-id\":\"some-key-id\","
                + "\"blob-metadata\":[{\"type\":\"some-stats\",\"snapshot-id\":42,"
                + "\"sequence-number\":3,\"fields\":[1,2],"
                + "\"properties\":{\"prop\":\"value\"}}]}");

    assertThat(JsonUtil.parse(json, StatisticsFileParser::fromJson)).isEqualTo(statisticsFile);
  }

  @Test
  void keyIdIsOmittedWhenNull() {
    StatisticsFile statisticsFile =
        new GenericStatisticsFile(42L, "/stats/file.puffin", 124L, 27L, null, BLOB_METADATA);

    String json = StatisticsFileParser.toJson(statisticsFile);
    assertThat(json).doesNotContain("key-id");
    assertThat(JsonUtil.parse(json, StatisticsFileParser::fromJson)).isEqualTo(statisticsFile);
  }

  @Test
  void missingKeyIdIsParsedAsNull() {
    String json =
        "{\"snapshot-id\":42,\"statistics-path\":\"/stats/file.puffin\","
            + "\"file-size-in-bytes\":124,\"file-footer-size-in-bytes\":27,\"blob-metadata\":[]}";

    StatisticsFile statisticsFile = JsonUtil.parse(json, StatisticsFileParser::fromJson);
    assertThat(statisticsFile.keyId()).isNull();
  }
}

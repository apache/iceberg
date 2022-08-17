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

package org.apache.iceberg.view;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.util.TestJsonUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestViewMetadataParser extends ParserTestBase<ViewMetadata> {

  private static ViewVersion version1 = BaseViewVersion.builder()
      .versionId(1)
      .timestampMillis(4353L)
      .addRepresentation(BaseViewDefinition.builder()
          .sql("select 'foo' foo")
          .build())
      .build();
  private static ViewHistoryEntry historyEntry1 = BaseViewHistoryEntry.of(4353L, 1);

  private static ViewVersion version2 = BaseViewVersion.builder()
      .versionId(2)
      .timestampMillis(5555L)
      .addRepresentation(BaseViewDefinition.builder()
          .sql("select 1 id, 'abc' data")
          .build())
      .build();
  private static ViewHistoryEntry historyEntry2 = BaseViewHistoryEntry.of(5555L, 2);

  private static TestJsonUtil.JsonStringWriter<ViewVersion> versionJsonStringWriter =
      TestJsonUtil.jsonStringWriter(ViewVersionParser::toJson);

  private static TestJsonUtil.JsonStringWriter<ViewHistoryEntry> historyEntryJsonStringWriter =
      TestJsonUtil.jsonStringWriter(ViewHistoryEntryParser::toJson);

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        {
            ViewMetadata.builder()
                .location("/location/dummy")
                .currentVersionId(2)
                .versions(Stream.of(version1, version2).collect(Collectors.toList()))
                .history(Stream.of(historyEntry1, historyEntry2).collect(Collectors.toList()))
                .build(),
            TestJsonUtil.objectString(
                "\"format-version\":1",
                "\"location\":\"/location/dummy\"",
                "\"properties\":{}",
                "\"current-version-id\":2",
                "\"versions\":" +
                    Stream.of(version1, version2)
                        .map(versionJsonStringWriter::write)
                        .collect(TestJsonUtil.joiningJsonArray()),
                "\"version-log\":" +
                    Stream.of(historyEntry1, historyEntry2)
                        .map(historyEntryJsonStringWriter::write)
                        .collect(TestJsonUtil.joiningJsonArray()))
        }
    });
  }

  public TestViewMetadataParser(ViewMetadata entry, String json) {
    super(entry, json, ViewMetadataParser::toJson, ViewMetadataParser::fromJson);
  }
}

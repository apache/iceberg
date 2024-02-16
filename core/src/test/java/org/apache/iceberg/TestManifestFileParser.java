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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.TestTemplate;

public class TestManifestFileParser extends TestBase {

  @TestTemplate
  public void testParser() throws Exception {
    ManifestFile manifest = createManifestFile();
    String jsonStr = JsonUtil.generate(gen -> ManifestFileParser.toJson(manifest, gen), false);
    assertThat(jsonStr).isEqualTo(manifestFileJson());
  }

  static ManifestFile createManifestFile() {
    ByteBuffer lowerBound = Conversions.toByteBuffer(Types.IntegerType.get(), 10);
    ByteBuffer upperBound = Conversions.toByteBuffer(Types.IntegerType.get(), 100);
    List<ManifestFile.PartitionFieldSummary> partitionFieldSummaries =
        Arrays.asList(new GenericPartitionFieldSummary(true, false, lowerBound, upperBound));
    ByteBuffer keyMetadata = Conversions.toByteBuffer(Types.IntegerType.get(), 987);

    return new GenericManifestFile(
        "/path/input.m0.avro",
        5878L,
        0,
        ManifestContent.DATA,
        1L,
        2L,
        12345678901234567L,
        1,
        10L,
        3,
        30L,
        0,
        0L,
        partitionFieldSummaries,
        keyMetadata);
  }

  private String manifestFileJson() {
    return "{\"path\":\"/path/input.m0.avro\","
        + "\"length\":5878,\"partition-spec-id\":0,\"content\":0,\"sequence-number\":1,\"min-sequence-number\":2,"
        + "\"added-snapshot-id\":12345678901234567,\"added-files-count\":1,\"existing-files-count\":3,\"deleted-files-count\":0,"
        + "\"added-rows-count\":10,\"existing-rows-count\":30,\"deleted-rows-count\":0,"
        + "\"partition-field-summary\":[{\"contains-null\":true,\"contains-nan\":false,"
        + "\"lower-bound\":\"0A000000\",\"upper-bound\":\"64000000\"}],"
        + "\"key-metadata\":\"DB030000\"}";
  }
}

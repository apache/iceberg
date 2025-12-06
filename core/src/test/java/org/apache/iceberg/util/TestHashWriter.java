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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonGenerator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestHashWriter {

  @Test
  public void testIncrementalHashCalculation() throws Exception {
    HashWriter hashWriter = spy(new HashWriter("SHA-256", StandardCharsets.UTF_8));

    // Create large enough TableMetadata which will be serialized into JSON in multiple chunks by
    // the JSON generator
    Map<String, String> icebergTblProperties = Maps.newHashMap();
    for (int i = 0; i < 300; ++i) {
      icebergTblProperties.put("Property Key " + i, "Property Value " + i);
    }
    Schema schema = new Schema(Types.NestedField.required(1, "col1", Types.StringType.get()));
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), null, icebergTblProperties);

    JsonGenerator generator = JsonUtil.factory().createGenerator(hashWriter);
    TableMetadataParser.toJson(tableMetadata, generator);

    // Expecting to see 3 write() invocations (and therefore incremental hash calculations)
    verify(hashWriter, times(3)).write(any(char[].class), anyInt(), anyInt());

    // +1 after flushing
    generator.flush();
    verify(hashWriter, times(4)).write(any(char[].class), anyInt(), anyInt());

    // Expected hash is calculated on the whole object i.e. without streaming
    byte[] expectedHash =
        MessageDigest.getInstance("SHA-256")
            .digest(TableMetadataParser.toJson(tableMetadata).getBytes(StandardCharsets.UTF_8));
    assertThat(hashWriter.getHash()).isEqualTo(expectedHash);
    assertThatThrownBy(() -> hashWriter.getHash()).hasMessageContaining("HashWriter is closed.");
  }
}

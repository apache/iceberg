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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class UnpartitionedDeltaWriterTest extends BaseWriterTest {

  @Test
  public void testUnpartitionedDeltaWriter() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.isUpsertMode()).thenReturn(true);
    when(config.tableConfig(table.name()))
        .thenReturn(new TableSinkConfig(Pattern.compile(""), List.of(), List.of(), ""));

    Record row = GenericRecord.create(SCHEMA);
    row.setField("id", 123L);
    row.setField("data", "hello world!");
    row.setField("id2", 123L);

    WriteResult result = writeTest(ImmutableList.of(row), config, UnpartitionedDeltaWriter.class);

    // in upsert mode, each write is a delete + append, so we'll have 1 data file
    // and 1 delete file
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);
  }
}

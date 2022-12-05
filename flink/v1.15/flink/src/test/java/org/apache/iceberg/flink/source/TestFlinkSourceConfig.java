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
package org.apache.iceberg.flink.source;

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkSourceConfig extends TestFlinkTableSource {
  private static final String TABLE = "test_table";

  @Test
  public void testFlinkSessionConfig() {
    getTableEnv().getConfig().set("streaming", "true");
    AssertHelpers.assertThrows(
        "Should throw exception because of cannot set snapshot-id option for streaming reader",
        IllegalArgumentException.class,
        "Cannot set as-of-timestamp option for streaming reader",
        () -> {
          sql("SELECT * FROM %s /*+ OPTIONS('as-of-timestamp'='1')*/", TABLE);
          return null;
        });

    List<Row> result =
        sql(
            "SELECT * FROM %s /*+ OPTIONS('as-of-timestamp'='%d','streaming'='false')*/",
            TABLE, System.currentTimeMillis());
    Assert.assertEquals(3, result.size());
  }
}

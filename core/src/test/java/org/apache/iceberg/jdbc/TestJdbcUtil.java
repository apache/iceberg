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
package org.apache.iceberg.jdbc;

import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestJdbcUtil {

  @Test
  public void testFilterAndRemovePrefix() {
    Map<String, String> input = Maps.newHashMap();
    input.put("warehouse", "/tmp/warehouse");
    input.put("user", "foo");
    input.put("jdbc.user", "bar");
    input.put("jdbc.pass", "secret");
    input.put("jdbc.jdbc.abcxyz", "abcxyz");

    Properties expected = new Properties();
    expected.put("user", "bar");
    expected.put("pass", "secret");
    expected.put("jdbc.abcxyz", "abcxyz");

    Properties actual = JdbcUtil.filterAndRemovePrefix(input, "jdbc.");

    Assertions.assertThat(expected).isEqualTo(actual);
  }
}

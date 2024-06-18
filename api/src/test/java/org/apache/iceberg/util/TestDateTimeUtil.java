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

import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

public class TestDateTimeUtil {

  @Test
  public void formatTimestampMillis() {
    String timestamp = "1970-01-01T00:00:00.001+00:00";
    assertThat(DateTimeUtil.formatTimestampMillis(1L)).isEqualTo(timestamp);
    assertThat(ZonedDateTime.parse(timestamp).toInstant().toEpochMilli()).isEqualTo(1L);

    timestamp = "1970-01-01T00:16:40+00:00";
    assertThat(DateTimeUtil.formatTimestampMillis(1000000L)).isEqualTo(timestamp);
    assertThat(ZonedDateTime.parse(timestamp).toInstant().toEpochMilli()).isEqualTo(1000000L);
  }
}

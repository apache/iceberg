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
package org.apache.iceberg.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestStatsUtil {

  @Test
  public void statsIdsForTableColumns() {
    int offset = 0;
    for (int i = 0; i < 7_084_500; i++) {
      long fieldId = StatsUtil.statsFieldIdFor(i);
      int expected = StatsUtil.DATA_SPACE_FIELD_ID_START + offset;
      assertThat(fieldId).as("at pos %s", i).isEqualTo(expected);
      offset = offset + StatsUtil.RESERVED_FIELD_IDS;
    }
  }

  @Test
  public void statsIdsForReservedColumns() {
    int offset = 0;
    for (int i = StatsUtil.RESERVED_FIELD_IDS_START; i < Integer.MAX_VALUE; i++) {
      long fieldId = StatsUtil.statsFieldIdFor(i);
      int expected = StatsUtil.METADATA_SPACE_FIELD_ID_START + offset;
      assertThat(fieldId).isEqualTo(expected);
      offset = offset + StatsUtil.RESERVED_FIELD_IDS;
    }
  }
}

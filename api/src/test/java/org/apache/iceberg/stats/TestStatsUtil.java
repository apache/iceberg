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

import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

public class TestStatsUtil {

  @Test
  public void statsIdsForTableColumns() {
    int offset = 0;
    // 100_000 + 200 * 7_084_500 = 1_417_000_000, which is the starting range for reserved columns
    for (int id = 0; id < 7_084_500; id++) {
      int statsFieldId = StatsUtil.statsFieldIdFor(id);
      int expected = StatsUtil.DATA_SPACE_FIELD_ID_START + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset = offset + StatsUtil.RESERVED_FIELD_IDS;
      assertThat(StatsUtil.fieldIdFor(statsFieldId)).as("at pos %s", id).isEqualTo(id);
    }
  }

  @Test
  public void statsIdsOverflowForTableColumns() {
    for (int i = 0; i < 100; i++) {
      int id =
          ThreadLocalRandom.current()
              .nextInt(StatsUtil.METADATA_SPACE_FIELD_ID_START, StatsUtil.RESERVED_FIELD_IDS_START);
      int statsFieldId = StatsUtil.statsFieldIdFor(id);
      int expected = -1;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      assertThat(StatsUtil.fieldIdFor(id)).as("at pos %s", id).isEqualTo(expected);
      assertThat(StatsUtil.fieldIdFor(statsFieldId)).as("at pos %s", id).isEqualTo(expected);
    }
  }

  @Test
  public void statsIdsForReservedColumns() {
    int offset = 0;
    for (int id = StatsUtil.RESERVED_FIELD_IDS_START; id < Integer.MAX_VALUE; id++) {
      int statsFieldId = StatsUtil.statsFieldIdFor(id);
      int expected = StatsUtil.METADATA_SPACE_FIELD_ID_START + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset = offset + StatsUtil.RESERVED_FIELD_IDS;
      assertThat(StatsUtil.fieldIdFor(statsFieldId)).as("at pos %s", id).isEqualTo(id);
    }
  }
}

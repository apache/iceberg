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

package org.apache.iceberg.flink;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;

public class AssertHelpers {
  private AssertHelpers() {
  }

  /**
   * Since flink's time type will truncate the microseconds and only keep millisecond, so when reading Record from
   * the file which was written by flink writer (Orc, Parquet, Avro), its time only have millisecond. We need to use
   * this method to assert records.
   *
   * @param expected the expected record
   * @param actual   the actual record.
   */
  public static void assertRecordEquals(Record expected, Record actual) {
    Record expectedRecord = (Record) truncateTimeToMillis(expected);
    Record actualRecord = (Record) truncateTimeToMillis(actual);
    Assert.assertEquals(expectedRecord, actualRecord);
  }

  private static Object truncateTimeToMillis(Object object) {
    if (object == null) {
      return null;
    } else if (object instanceof List) {
      List<?> list = (List<?>) object;
      List<Object> result = Lists.newArrayList();
      for (Object element : list) {
        result.add(truncateTimeToMillis(element));
      }
      return result;
    } else if (object instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) object;
      Map<Object, Object> result = Maps.newHashMap();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        result.put(truncateTimeToMillis(entry.getKey()), truncateTimeToMillis(entry.getValue()));
      }
      return result;
    } else if (object instanceof Record) {
      Record record = (Record) object;
      Record result = record.copy();
      for (int i = 0; i < record.size(); i++) {
        result.set(i, truncateTimeToMillis(record.get(i)));
      }
      return result;
    } else if (object instanceof LocalTime) {
      LocalTime localTime = (LocalTime) object;
      // Truncate the microseconds.
      return LocalTime.ofNanoOfDay(localTime.toNanoOfDay() / 1_000_000 * 1_000);
    } else {
      return object;
    }
  }
}

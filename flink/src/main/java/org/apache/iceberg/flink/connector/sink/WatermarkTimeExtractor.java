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

package org.apache.iceberg.flink.connector.sink;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkTimeExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkTimeExtractor.class);

  private final TimeUnit timestampUnit;
  private final List<String> timestampFieldChain;

  public WatermarkTimeExtractor(Schema schema, String timestampFieldChainAsString, TimeUnit timestampUnit) {
    this.timestampUnit = timestampUnit;
    this.timestampFieldChain = getTimestampFieldChain(schema, timestampFieldChainAsString);
  }

  private List<String> getTimestampFieldChain(Schema schema, String timestampFieldChainAsString) {
    if (Strings.isNullOrEmpty(timestampFieldChainAsString)) {
      return Collections.emptyList();
    }

    List<String> fieldNames = Splitter.on(".").splitToList(timestampFieldChainAsString);
    final int size = fieldNames.size();  // size >= 1 due to the Strings.isNullOrEmpty() check ahead
    Type type = null;
    for (int i = 0; i <= size - 1; i++) {  // each field in the chain
      String fieldName = fieldNames.get(i).trim();

      if (i == 0) {  // first level
        type = schema.findType(fieldName);
      } else {  // other levels including leaf
        type = type.asNestedType().fieldType(fieldName);
      }

      if (type == null) {
        throw new IllegalArgumentException(
            String.format("Can't find field %s in schema", fieldName));
      } else {
        if (i == size - 1) {  // leaf node should be timestamp type
          if (type.typeId() != Type.TypeID.TIME) {   // TODO: to use TimestampType ？
            throw new IllegalArgumentException(
                String.format("leaf timestamp field %s is not a timestamp type, but %s", fieldName, type.typeId()));
          }
        } else {
          if (!type.isNestedType()) {
            throw new IllegalArgumentException(
                String.format("upstream field %s is not a nested type, but %s", fieldName, type.typeId()));
          }
        }
      }
    }  // each field in the chain

    LOG.info("Found matched timestamp field identified by {} in the schema", timestampFieldChainAsString);
    return fieldNames;
  }

  /**
   * @return null if timestamp field not found in the record
   */
  public Long getWatermarkTimeMs(final Record record) {
    if (timestampFieldChain.isEmpty()) {
      return null;
    }

    Record recordAlongPath = record;

    // from top to the parent of leaf
    for (int i = 0; i <= timestampFieldChain.size() - 2; i++) {
      recordAlongPath = (Record) recordAlongPath.getField(timestampFieldChain.get(i));
    }

    // leaf
    LocalTime ts = (LocalTime) recordAlongPath.getField(timestampFieldChain.get(timestampFieldChain.size() - 1));
    if (ts == null) {
      return null;
    } else {
      long tsInMills = ts.toNanoOfDay() / 1000;
      if (timestampUnit != TimeUnit.MILLISECONDS) {
        return timestampUnit.toMillis(tsInMills);
      } else {
        return tsInMills;
      }
    }
  }
}

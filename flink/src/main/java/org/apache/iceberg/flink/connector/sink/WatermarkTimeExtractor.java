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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkTimeExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkTimeExtractor.class);

  private final TimeUnit vttsTimestampUnit;
  private final List<Schema.Field> timestampFieldChain;

  /**
   * @param vttsTimestampField can include multiple fields as comma separated list, e.g. "fieldA,fieldB".
   *                           Each field can also use '.' to describe hierarchy chaining,
   *                           e.g. "topFieldA.secondLevelFieldB".
   */
  public WatermarkTimeExtractor(Schema avroSchema, String vttsTimestampField, TimeUnit vttsTimestampUnit) {
    this.vttsTimestampUnit = vttsTimestampUnit;
    this.timestampFieldChain = getTimestampFieldChain(avroSchema, vttsTimestampField);
  }

  private List<Schema.Field> getTimestampFieldChain(Schema avroSchema, String vttsTimestampField) {
    if (Strings.isNullOrEmpty(vttsTimestampField)) {
      return Collections.emptyList();
    }
    for (String fieldNameChainStr : Splitter.on(",").splitToList(vttsTimestampField)) {
      List<Schema.Field> fieldChain = new ArrayList<>();
      Schema currSchema = avroSchema;
      List<String> fieldNameChain = Splitter.on(".").splitToList(fieldNameChainStr);
      for (int i = 0; i < fieldNameChain.size(); ++i) {
        String fieldName = fieldNameChain.get(i).trim();
        Schema.Field field = currSchema.getField(fieldName);
        if (null == field) {
          LOG.info("Can't find field {} in Schema for chain {}", fieldName, fieldNameChain);
          break;
        } else {
          currSchema = AvroUtils.getActualSchema(field.schema());
          // validate field type
          if (i == fieldNameChain.size() - 1) {
            // leaf node should be long type
            if (Schema.Type.LONG != currSchema.getType()) {
              throw new IllegalArgumentException(
                  String.format("leaf timestamp field %s is not a long type: %s", fieldName, currSchema.getType()));
            }
          } else {
            // upstream nodes should be record type
            if (Schema.Type.RECORD != currSchema.getType()) {
              throw new IllegalArgumentException(
                  String.format("upstream field %s is not a record type: %s", fieldName, currSchema.getType()));
            }
          }
          fieldChain.add(field);
        }
      }
      if (fieldNameChain.size() == fieldChain.size()) {
        LOG.info("Found the match field in schema: {}", fieldNameChainStr);
        return fieldChain;
      }
    }
    throw new IllegalArgumentException("Can't find timestamp field in schema: " + vttsTimestampField);
  }

  /**
   * @return null if timestamp field not found in the record
   */
  public Long getWatermarkTimeMs(Record record) {
    if (timestampFieldChain.isEmpty()) {
      return null;
    }
    // traverse to find the leaf node first
    Record leafRecord = record;
    for (int i = 0; i < timestampFieldChain.size() - 1; ++i) {
      leafRecord = (Record) leafRecord.get(timestampFieldChain.get(i).pos());
      if (null == leafRecord) {
        return null;
      }
    }
    Long ts = (Long) leafRecord.get(timestampFieldChain.get(timestampFieldChain.size() - 1).pos());
    if (null != ts && TimeUnit.MILLISECONDS != vttsTimestampUnit) {
      return vttsTimestampUnit.toMillis(ts);
    } else {
      return ts;
    }
  }
}

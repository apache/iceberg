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
package io.tabular.iceberg.connect.transforms;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DmsTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DmsTransform.class.getName());
  private static final ConfigDef EMPTY_CONFIG = new ConfigDef();

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      throw new UnsupportedOperationException("Schema not support for DMS records");
    }
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "DMS transform");

    // promote fields under "data"
    Object dataObj = value.get("data");
    Object metadataObj = value.get("metadata");
    if (!(dataObj instanceof Map) || !(metadataObj instanceof Map)) {
      LOG.debug("Unable to transform DMS record, skipping...");
      return null;
    }

    Map<String, Object> result = Maps.newHashMap((Map<String, Object>) dataObj);

    Map<String, Object> metadata = (Map<String, Object>) metadataObj;

    String dmsOp = metadata.get("operation").toString();
    String op;
    switch (dmsOp) {
      case "update":
        op = CdcConstants.OP_UPDATE;
        break;
      case "delete":
        op = CdcConstants.OP_DELETE;
        break;
      default:
        op = CdcConstants.OP_INSERT;
    }

    result.put(CdcConstants.COL_CDC_OP, op);
    result.put(
        CdcConstants.COL_CDC_TABLE,
        String.format("%s.%s", metadata.get("schema-name"), metadata.get("table-name")));
    result.put(CdcConstants.COL_CDC_TS, metadata.get("timestamp"));

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        result,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return EMPTY_CONFIG;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}

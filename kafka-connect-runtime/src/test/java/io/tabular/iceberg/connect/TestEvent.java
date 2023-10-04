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
package io.tabular.iceberg.connect;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;

// JavaBean-style for serialization
public class TestEvent {

  public static final Schema TEST_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "type", Types.StringType.get()),
              Types.NestedField.required(3, "ts", Types.TimestampType.withZone()),
              Types.NestedField.required(4, "payload", Types.StringType.get())),
          ImmutableSet.of(1));

  public static final PartitionSpec TEST_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).day("ts").build();

  private final long id;
  private final String type;
  private final long ts;
  private final String payload;
  private final String op;

  public TestEvent(long id, String type, long ts, String payload) {
    this(id, type, ts, payload, null);
  }

  public TestEvent(long id, String type, long ts, String payload, String op) {
    this.id = id;
    this.type = type;
    this.ts = ts;
    this.payload = payload;
    this.op = op;
  }

  public long getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public long getTs() {
    return ts;
  }

  public String getPayload() {
    return payload;
  }

  public String getOp() {
    return op;
  }
}

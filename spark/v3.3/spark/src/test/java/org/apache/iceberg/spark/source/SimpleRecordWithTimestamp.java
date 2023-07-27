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

package org.apache.iceberg.spark.source;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class SimpleRecordWithTimestamp extends SimpleRecord {
  private Instant timestamp;

  public SimpleRecordWithTimestamp() {
  }

  public SimpleRecordWithTimestamp(Integer id, Instant ts) {
    super(id, String.valueOf(id));
    this.timestamp = ts.plus(id-1, ChronoUnit.DAYS);
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SimpleRecordWithTimestamp that = (SimpleRecordWithTimestamp) o;
    return Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timestamp);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(getId());
    buffer.append(",\"data\"=\"");
    buffer.append(getData());
    buffer.append(",\"timestamp\"=\"");
    buffer.append(timestamp);
    buffer.append("\"}");
    return buffer.toString();
  }
}

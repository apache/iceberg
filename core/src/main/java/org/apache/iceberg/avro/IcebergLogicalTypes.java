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
package org.apache.iceberg.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class IcebergLogicalTypes {

  private IcebergLogicalTypes() {}

  private static final String TIMESTAMP_NANOS = "timestamp-nanos";

  private static final TimestampNanos TIMESTAMP_NANOS_TYPE = new TimestampNanos();

  public static TimestampNanos timestampNanos() {
    return TIMESTAMP_NANOS_TYPE;
  }

  /** TimestampNanos represents a date and time in nanoseconds */
  public static class TimestampNanos extends LogicalType {
    private TimestampNanos() {
      super(TIMESTAMP_NANOS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException(
            "Timestamp (nanos) can only be used with an underlying long type");
      }
    }
  }
}

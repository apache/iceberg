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
package org.apache.iceberg.flink.sink.dynamic;

import org.apache.flink.annotation.Experimental;

/**
 * Thrown when a {@link DynamicRecord} cannot be written because of the record itself, for example a
 * null value for a required column, an invalid upsert configuration, an equality field that is not
 * part of the schema, or a schema change that the table cannot take. Processing the same record
 * again fails the same way, which makes it safe to divert to the error stream configured with
 * {@link DynamicIcebergSink.Builder#errorStreamConsumer}.
 */
@Experimental
public class DynamicRecordException extends RuntimeException {

  public DynamicRecordException(String message) {
    super(message);
  }

  public DynamicRecordException(String message, Throwable cause) {
    super(message, cause);
  }
}

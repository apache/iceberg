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

import java.io.Serializable;
import org.apache.flink.annotation.Experimental;

/**
 * Classifies a failure raised while processing a {@link DynamicRecord} as divertable or not. Only
 * failures this filter accepts are diverted to the error stream set via {@link
 * DynamicIcebergSink.Builder#errorStreamConsumer}; every other failure propagates and fails the job
 * so that Flink's restart strategy can retry it.
 *
 * <p>The failures caused by the record itself are thrown as {@link DynamicRecordException}, so
 * {@code error -> error instanceof DynamicRecordException} diverts exactly those. Accept other
 * types only for failures that are deterministic for the record, never for transient or systemic
 * ones such as an unreachable catalog, object-store errors or an interrupt, otherwise a sustained
 * outage drains the whole stream.
 */
@Experimental
@FunctionalInterface
public interface DynamicRecordExceptionFilter extends Serializable {

  /**
   * Classifies a failure as divertable or not.
   *
   * @param error the cause thrown while processing a record
   * @return {@code true} to divert the record, {@code false} to propagate the failure and fail the
   *     job
   */
  boolean shouldDivert(Exception error);
}

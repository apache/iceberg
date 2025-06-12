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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.util.Collector;

/** Conversion method to return input type into a DynamicRecord */
public interface DynamicRecordConverter<T> extends Serializable {
  default void open(OpenContext openContext) throws Exception {}

  /** Takes a user-defined input type and converts it one or multiple {@link DynamicRecord}s. */
  void convert(T inputRecord, Collector<DynamicRecord> out) throws Exception;
}

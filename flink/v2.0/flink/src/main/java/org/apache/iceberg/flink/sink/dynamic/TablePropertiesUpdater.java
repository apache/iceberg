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
import java.util.Map;

@FunctionalInterface
public interface TablePropertiesUpdater extends Serializable {

  /**
   * Updates table properties based on the current properties and the fully-qualified table name.
   *
   * @param tableName the fully-qualified name of the table being updated
   * @param currentProperties the current table properties
   * @return the updated table properties
   */
  Map<String, String> apply(String tableName, Map<String, String> currentProperties);
}

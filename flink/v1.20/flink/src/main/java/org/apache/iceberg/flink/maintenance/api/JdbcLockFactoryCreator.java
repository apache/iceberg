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
package org.apache.iceberg.flink.maintenance.api;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class JdbcLockFactoryCreator implements BaseLockFactoryCreator {

  public static final String JDBC_LOCK = "jdbc";
  public static final String JDBC_URI = LOCK_PREFIX + "." + JDBC_LOCK + ".uri";

  @Override
  public TriggerLockFactory create(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(JDBC_URI), "JDBC lock requires %s parameter", JDBC_URI);
    Preconditions.checkArgument(
        config.containsKey(BaseLockFactoryCreator.LOCK_ID),
        "JDBC lock requires %s parameter",
        BaseLockFactoryCreator.LOCK_ID);
    return new JdbcLockFactory(
        config.get(JDBC_URI), config.get(BaseLockFactoryCreator.LOCK_ID), config);
  }
}

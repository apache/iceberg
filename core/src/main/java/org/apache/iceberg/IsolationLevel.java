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
package org.apache.iceberg;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An isolation level in a table.
 *
 * <p>Two isolation levels are supported: serializable and snapshot isolation. Both of them provide
 * a read consistent view of the table to all operations and allow readers to see only already
 * committed data. While serializable is the strongest isolation level in databases, snapshot
 * isolation is beneficial for environments with many concurrent writers.
 *
 * <p>The serializable isolation level guarantees that an ongoing UPDATE/DELETE/MERGE operation
 * fails if a concurrent transaction commits a new file that might contain rows matching the
 * condition used in UPDATE/DELETE/MERGE. For example, if there is an ongoing update on a subset of
 * rows and a concurrent transaction adds a new file with records that potentially match the update
 * condition, the update operation must fail under the serializable isolation but can still commit
 * under the snapshot isolation.
 */
public enum IsolationLevel {
  SERIALIZABLE,
  SNAPSHOT;

  public static IsolationLevel fromName(String levelName) {
    Preconditions.checkArgument(levelName != null, "Invalid isolation level: null");
    try {
      return IsolationLevel.valueOf(levelName.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid isolation level: %s", levelName), e);
    }
  }
}

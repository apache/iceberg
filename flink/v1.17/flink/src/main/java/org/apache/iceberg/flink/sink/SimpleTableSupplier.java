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
package org.apache.iceberg.flink.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * A table loader that will only reload a table after a certain interval has passed. WARNING: This
 * table loader should be used carefully when used with writer tasks. It could result in heavy load
 * on a catalog for jobs with many writers.
 */
@Internal
public class SimpleTableSupplier implements SerializableSupplier<Table> {
  private final SerializableTable table;

  SimpleTableSupplier(SerializableTable initialTable) {
    Preconditions.checkArgument(initialTable != null, "initialTable cannot be null");
    this.table = initialTable;
  }

  @Override
  public Table get() {
    return table;
  }
}

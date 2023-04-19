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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Generic interface for creating and loading a table implementation.
 *
 * <p>The 'tableIdentifier' field should be interpreted by the underlying implementation (e.g.
 * database.table_name)
 */
public interface Tables {
  default Table create(Schema schema, String tableIdentifier) {
    return create(schema, PartitionSpec.unpartitioned(), ImmutableMap.of(), tableIdentifier);
  }

  default Table create(Schema schema, PartitionSpec spec, String tableIdentifier) {
    return create(schema, spec, ImmutableMap.of(), tableIdentifier);
  }

  default Table create(
      Schema schema, PartitionSpec spec, Map<String, String> properties, String tableIdentifier) {
    return create(schema, spec, SortOrder.unsorted(), properties, tableIdentifier);
  }

  default Table create(
      Schema schema,
      PartitionSpec spec,
      SortOrder order,
      Map<String, String> properties,
      String tableIdentifier) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement create with a sort order");
  }

  Table load(String tableIdentifier);

  boolean exists(String tableIdentifier);
}

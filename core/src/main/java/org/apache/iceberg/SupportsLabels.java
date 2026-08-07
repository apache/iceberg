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

import org.apache.iceberg.rest.labels.Labels;

/**
 * Implemented by tables that can expose catalog-provided labels obtained from the load response.
 *
 * <p>Labels are optional, catalog-provided metadata enrichment. They are not part of table state
 * and are not preserved when the table is serialized.
 */
public interface SupportsLabels {
  /**
   * Returns the catalog-provided labels for this table, or an empty instance when there are none.
   */
  Labels labels();
}

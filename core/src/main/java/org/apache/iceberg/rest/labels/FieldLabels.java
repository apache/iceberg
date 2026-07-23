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
package org.apache.iceberg.rest.labels;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/**
 * Catalog-provided labels attached to a single field, identified by its field id.
 *
 * <p>Labels are ephemeral, catalog-provided metadata enrichment returned on the read path. They are
 * not part of table state. See {@link Labels}.
 */
@Value.Immutable
public interface FieldLabels {
  int fieldId();

  Map<String, String> labels();

  @Value.Check
  default void validate() {
    Preconditions.checkArgument(fieldId() >= 1, "Invalid field id: must be >= 1");
    Preconditions.checkArgument(!labels().isEmpty(), "Invalid labels: must be non-empty");
  }
}

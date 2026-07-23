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

import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Catalog-provided metadata enrichment (for example ownership, classification, or cost attribution)
 * returned alongside a table or view on the read path.
 *
 * <p>Labels are generated per request and optional; clients may ignore them. They are not part of
 * table state: the spec does not require how a catalog produces or stores labels, nor whether they
 * are persisted or versioned. {@link #object()} carries catalog-object-level labels; {@link
 * #fields()} carries per-field labels, each identified by field id.
 */
@Value.Immutable
public interface Labels {
  /** Catalog-object-level labels, attached to the object (table, view, ...) as a whole. */
  Map<String, String> object();

  /** Field-level labels. Each entry identifies its field by field id. */
  List<FieldLabels> fields();
}

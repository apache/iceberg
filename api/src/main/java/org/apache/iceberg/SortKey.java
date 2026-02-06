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

import java.util.List;
import java.util.stream.Collectors;

/**
 * A struct of flattened sort field values.
 *
 * <p>Instances of this class can produce sort values from a row passed to {@link
 * #wrap(StructLike)}.
 */
public class SortKey extends StructTransform {
  private final Schema schema;
  private final SortOrder sortOrder;

  public SortKey(Schema schema, SortOrder sortOrder) {
    super(schema, fieldTransform(sortOrder));
    this.schema = schema;
    this.sortOrder = sortOrder;
  }

  private SortKey(SortKey toCopy) {
    // only need deep copy inside StructTransform
    super(toCopy);
    this.schema = toCopy.schema;
    this.sortOrder = toCopy.sortOrder;
  }

  public SortKey copy() {
    return new SortKey(this);
  }

  private static List<FieldTransform> fieldTransform(SortOrder sortOrder) {
    return sortOrder.fields().stream()
        .map(sortField -> new FieldTransform(sortField.sourceId(), sortField.transform()))
        .collect(Collectors.toList());
  }
}

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
import java.util.Map;
import org.immutables.value.Value;

/** Optional catalog-provided labels returned on a load response. */
@Value.Immutable
public interface Labels {
  Labels EMPTY = ImmutableLabels.builder().build();

  /** Object-level labels. */
  Map<String, String> objectLabels();

  /** Field-level labels */
  List<FieldLabels> fields();

  /** Returns true when there are neither object-level nor field-level labels. */
  default boolean isEmpty() {
    return objectLabels().isEmpty() && fields().isEmpty();
  }
}

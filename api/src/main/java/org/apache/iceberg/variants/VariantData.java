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
package org.apache.iceberg.variants;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class VariantData implements Variant {
  private static final Joiner DOT_JOINER = Joiner.on(".");

  private final VariantMetadata metadata;
  private final VariantValue value;

  VariantData(VariantMetadata metadata, VariantValue value) {
    Preconditions.checkArgument(metadata != null, "Invalid variant metadata: null");
    Preconditions.checkArgument(value != null, "Invalid variant value: null");
    this.metadata = metadata;
    this.value = value;
  }

  @Override
  public VariantMetadata metadata() {
    return metadata;
  }

  @Override
  public VariantValue value() {
    return value;
  }

  @Override
  public VariantValue query(String path) {
    VariantValue current = value;
    List<String> resolved = Lists.newArrayList();
    for (String field : VariantDataUtil.parsePath(path)) {
      Preconditions.checkArgument(
          current != null && current.type() == PhysicalType.OBJECT,
          "Cannot find %s: %s=%s is not an object",
          path,
          DOT_JOINER.join(resolved),
          current);

      current = current.asObject().get(field);
      resolved.add(field);
    }

    return current;
  }
}

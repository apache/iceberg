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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.databind.InjectableValues;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class ParserContext {

  private final Map<String, Object> data;

  private ParserContext(Builder builder) {
    this.data = Collections.unmodifiableMap(builder.data);
  }

  public boolean isEmpty() {
    return data.isEmpty();
  }

  public InjectableValues toInjectableValues() {
    return new InjectableValues.Std(data);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, Object> data = Maps.newHashMap();

    private Builder() {}

    public Builder add(String key, Object value) {
      Preconditions.checkNotNull(key, "Key cannot be null");
      Preconditions.checkNotNull(value, "Value cannot be null");
      this.data.put(key, value);
      return this;
    }

    public ParserContext build() {
      return new ParserContext(this);
    }
  }
}

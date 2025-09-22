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

import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Represents an HTTP challenge according to RFC 7235.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7235#section-2.1">RFC 7235 Section 2.1</a>
 */
@Value.Style(depluralize = true)
@Value.Immutable
public interface HTTPChallenge {

  static HTTPChallenge of(String scheme, @Nullable String value, Map<String, String> params) {
    return ImmutableHTTPChallenge.builder().scheme(scheme).value(value).params(params).build();
  }

  /** Returns the challenge's scheme, such as "Basic" or "Bearer". */
  String scheme();

  /** Returns the challenge's token68 value, or null if none provided. */
  @Nullable
  String value();

  /** Returns the challenge's parameters, or an empty map if none provided. */
  Map<String, String> params();
}

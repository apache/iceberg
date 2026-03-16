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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;

class ETagProvider {
  private static final HashFunction MURMUR3 = Hashing.murmur3_32_fixed();

  private static final Joiner.MapJoiner PARAMS_JOINER = Joiner.on(",").withKeyValueSeparator("=");
  private static final Joiner COMMA = Joiner.on(',');

  private ETagProvider() {}

  public static String of(String metadataLocation, Map<String, String> params) {
    Preconditions.checkArgument(null != metadataLocation, "Invalid metadata location: null");
    Preconditions.checkArgument(!metadataLocation.isEmpty(), "Invalid metadata location: empty");

    String stringToHash = metadataLocation;
    if (params != null && !params.isEmpty()) {
      Map<String, String> orderedParams = new TreeMap<>(params);

      stringToHash = COMMA.join(metadataLocation, PARAMS_JOINER.join(orderedParams));
    }

    return MURMUR3.hashString(stringToHash, StandardCharsets.UTF_8).toString();
  }
}

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
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class DefaultETagProvider implements ETagProvider {
  private static final HashFunction MURMUR3 = Hashing.murmur3_32_fixed();

  @Override
  public String of(LoadTableResponse resp) {
    return MURMUR3.hashString(resp.metadataLocation(), StandardCharsets.UTF_8).toString();
  }
}

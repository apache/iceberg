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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Endpoint {

  private static final Splitter ENDPOINT_SPLITTER = Splitter.on(" ");
  private static final Joiner ENDPOINT_JOINER = Joiner.on(" ");

  public abstract String httpVerb();

  public abstract String resourcePath();

  public static Endpoint create(String httpVerb, String resourcePath) {
    return ImmutableEndpoint.builder().httpVerb(httpVerb).resourcePath(resourcePath).build();
  }

  @Override
  public String toString() {
    return ENDPOINT_JOINER.join(httpVerb(), resourcePath());
  }

  public static Endpoint fromString(String endpoint) {
    List<String> elements = ENDPOINT_SPLITTER.splitToList(endpoint);
    Preconditions.checkArgument(
        elements.size() == 2,
        "Invalid endpoint (must consist of two elements separated by space): %s",
        endpoint);
    return create(elements.get(0), elements.get(1));
  }
}

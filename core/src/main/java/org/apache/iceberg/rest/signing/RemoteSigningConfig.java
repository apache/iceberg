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
package org.apache.iceberg.rest.signing;

import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/** Configuration for remote signer clients. */
@Value.Immutable
public interface RemoteSigningConfig {

  RemoteSigningConfig EMPTY = ImmutableRemoteSigningConfig.builder().build();

  /**
   * Static key-value pairs the signer client MUST pass through unchanged in the {@code properties}
   * field of every {@code RemoteSignRequest} sent to the signing endpoint.
   */
  Map<String, String> properties();

  /**
   * Static headers the signer client MUST include unchanged in every request to the signing
   * endpoint.
   */
  Map<String, List<String>> headers();

  @Value.Derived
  default boolean isEmpty() {
    return properties().isEmpty() && headers().isEmpty();
  }
}

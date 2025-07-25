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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.http.RESTClientAdapter;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;

/**
 * A runtime context for OAuth2.
 *
 * <p>This component groups together dependencies that are not part of the OAuth2 configuration as
 * provided by the user in {@link OAuth2Config}, but rather are provided by the environment.
 */
@Value.Immutable
public interface OAuth2Runtime {

  static OAuth2Runtime of(
      Supplier<RESTClient> restClientSupplier,
      ScheduledExecutorService executor,
      @Nullable OAuth2Client parent) {
    return ImmutableOAuth2Runtime.builder()
        .httpClient(new RESTClientAdapter(restClientSupplier))
        .executor(executor)
        .parent(Optional.ofNullable(parent))
        .build();
  }

  /**
   * The {@link HTTPRequestSender} to use for network requests. In production, this is generally an
   * instance of {@link RESTClientAdapter}.
   */
  @Value.Default
  default HTTPRequestSender httpClient() {
    return request -> {
      if (request instanceof HTTPRequest req) {
        return req.send();
      }

      throw new IllegalArgumentException(
          "Default HTTPRequestSender only supports HTTPRequest instances, but got: "
              + request.getClass().getName());
    };
  }

  /** The executor to use for token refresh operations. */
  @Value.Default
  default ScheduledExecutorService executor() {
    return ThreadPools.authRefreshPool();
  }

  /**
   * The parent client, if any. This is used to inherit the parent's token in certain token exchange
   * scenarios.
   */
  Optional<OAuth2Client> parent();

  /**
   * The clock to use for time-based operations. Defaults to the system clock. Mostly used for
   * testing.
   */
  @Value.Default
  default Clock clock() {
    return Clock.systemUTC();
  }
}

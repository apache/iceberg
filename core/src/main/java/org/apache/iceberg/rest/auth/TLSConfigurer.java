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
package org.apache.iceberg.rest.auth;

import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.ssl.SSLContexts;

public interface TLSConfigurer {

  default void initialize(Map<String, String> properties) {}

  default SSLContext sslContext() {
    return SSLContexts.createDefault();
  }

  /**
   * Returns a custom {@link HostnameVerifier} to use for hostname verification, or {@code null} to
   * use the default JSSE built-in hostname verifier.
   *
   * <p>If a non-null verifier is returned, only the custom verifier is executed and the JSSE
   * built-in hostname verifier won't be executed.
   */
  @Nullable
  default HostnameVerifier hostnameVerifier() {
    return null;
  }

  default String[] supportedProtocols() {
    return null;
  }

  default String[] supportedCipherSuites() {
    return null;
  }
}

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
package org.apache.iceberg.aws.kms;

import java.io.Serializable;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

public class KmsClientProperties implements Serializable {
  public KmsClientProperties() {}

  /**
   * Set ADAPTIVE_V2 <a
   * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryMode.html">RetryMode</a>
   * for a KMS client.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/client/config/ClientOverrideConfiguration.Builder.html#retryStrategy(software.amazon.awssdk.core.retry.RetryMode)
   */
  public <T extends KmsClientBuilder> void applyRetryConfigurations(T builder) {
    ClientOverrideConfiguration.Builder configBuilder =
        null != builder.overrideConfiguration()
            ? builder.overrideConfiguration().toBuilder()
            : ClientOverrideConfiguration.builder();

    builder.overrideConfiguration(configBuilder.retryStrategy(RetryMode.ADAPTIVE_V2).build());
  }
}

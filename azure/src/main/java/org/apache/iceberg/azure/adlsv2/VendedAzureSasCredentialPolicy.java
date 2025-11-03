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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.AzureSasCredentialPolicy;
import com.azure.core.http.policy.HttpPipelinePolicy;
import reactor.core.publisher.Mono;

class VendedAzureSasCredentialPolicy implements HttpPipelinePolicy {
  private final String account;
  private final VendedAdlsCredentialProvider vendedAdlsCredentialProvider;
  private volatile AzureSasCredential azureSasCredential;
  private AzureSasCredentialPolicy azureSasCredentialPolicy;

  VendedAzureSasCredentialPolicy(
      String account, VendedAdlsCredentialProvider vendedAdlsCredentialProvider) {
    this.account = account;
    this.vendedAdlsCredentialProvider = vendedAdlsCredentialProvider;
  }

  @Override
  public Mono<HttpResponse> process(
      HttpPipelineCallContext httpPipelineCallContext,
      HttpPipelineNextPolicy httpPipelineNextPolicy) {
    return maybeUpdateCredential()
        .then(
            Mono.defer(
                () ->
                    azureSasCredentialPolicy.process(
                        httpPipelineCallContext, httpPipelineNextPolicy)));
  }

  @Override
  public HttpResponse processSync(
      HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
    maybeUpdateCredential().block();
    return azureSasCredentialPolicy.processSync(context, next);
  }

  private Mono<Void> maybeUpdateCredential() {
    return vendedAdlsCredentialProvider
        .credentialForAccount(account)
        .flatMap(
            sasToken -> {
              if (azureSasCredential == null) {
                synchronized (this) {
                  if (azureSasCredential == null) {
                    this.azureSasCredential = new AzureSasCredential(sasToken);
                    this.azureSasCredentialPolicy =
                        new AzureSasCredentialPolicy(azureSasCredential, false);
                    return Mono.empty();
                  }
                }
              }
              azureSasCredential.update(sasToken);
              return Mono.empty();
            });
  }
}

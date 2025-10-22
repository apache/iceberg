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
package org.apache.iceberg.aws;

import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * A delegating wrapper around SdkHttpClient that handles reference counting for lifecycle
 * management. When close() is called, it decrements the reference count and only closes the
 * underlying client when the count reaches zero.
 */
class WrappedSdkHttpClient implements SdkHttpClient {
  private final SdkHttpClient delegate;
  private final String clientKey;

  WrappedSdkHttpClient(SdkHttpClient delegate, String clientKey) {
    this.delegate = delegate;
    this.clientKey = clientKey;
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return delegate.prepareRequest(request);
  }

  @Override
  public void close() {
    // Delegate close to the registry which manages ref counting
    ManagedHttpClientRegistry.getInstance().releaseClient(clientKey);
  }

  @Override
  public String clientName() {
    return delegate.clientName();
  }

  @VisibleForTesting
  SdkHttpClient getDelegate() {
    return delegate;
  }

  @VisibleForTesting
  String getClientKey() {
    return clientKey;
  }
}

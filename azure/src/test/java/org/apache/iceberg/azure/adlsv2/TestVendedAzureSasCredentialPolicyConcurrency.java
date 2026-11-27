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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class TestVendedAzureSasCredentialPolicyConcurrency {

  private static final String STORAGE_ACCOUNT = "account1";
  private static final String URL = "https://account1.dfs.core.windows.net/container1/file1";
  private static final int ROUNDS = 20_000;

  @Test
  void firstRequestsFromManyThreads() throws Exception {
    int threads = Math.min(8, Runtime.getRuntime().availableProcessors());
    assumeThat(threads).as("Requires parallel threads").isGreaterThan(1);

    // A plain subclass instead of a mock: a mock serializes concurrent calls and hides the race.
    VendedAdlsCredentialProvider provider =
        new VendedAdlsCredentialProvider(
            Map.of(
                VendedAdlsCredentialProvider.URI,
                "http://localhost/credentials",
                CatalogProperties.URI,
                "http://localhost")) {
          @Override
          Mono<String> credentialForAccount(String storageAccount) {
            return Mono.just("tokenInstance=1");
          }
        };
    HttpResponse response = mock(HttpResponse.class);
    HttpClient httpClient = request -> Mono.just(response);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < ROUNDS; round++) {
        // every round uses a new policy, so all threads send their first request through it
        HttpPipeline pipeline =
            new HttpPipelineBuilder()
                .httpClient(httpClient)
                .policies(new VendedAzureSasCredentialPolicy(STORAGE_ACCOUNT, provider))
                .build();

        AtomicInteger ready = new AtomicInteger();
        AtomicBoolean start = new AtomicBoolean();
        List<Future<?>> requests = Lists.newArrayList();
        for (int thread = 0; thread < threads; thread++) {
          requests.add(
              executor.submit(
                  () -> {
                    HttpRequest request = new HttpRequest(HttpMethod.GET, URL);
                    ready.incrementAndGet();
                    while (!start.get()) {
                      Thread.onSpinWait();
                    }

                    return pipeline.send(request).block();
                  }));
        }

        while (ready.get() < threads) {
          Thread.onSpinWait();
        }

        start.set(true);
        for (Future<?> request : requests) {
          assertThatCode(request::get).doesNotThrowAnyException();
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }
}

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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;

class TestAnalyticsAcceleratorUtil {
  private static final int OPEN_FILE_IO_COUNT = 150;

  private final S3FileIOProperties properties = new S3FileIOProperties();
  private final List<S3AsyncClient> clients = Lists.newArrayList();

  @AfterEach
  void after() {
    clients.forEach(client -> AnalyticsAcceleratorUtil.cleanupCache(client, properties));
  }

  @Test
  void factoriesOfOpenFileIOsAreNotEvicted() {
    List<S3SeekableInputStreamFactory> factories = Lists.newArrayList();
    for (int i = 0; i < OPEN_FILE_IO_COUNT; i += 1) {
      S3AsyncClient client = Mockito.mock(S3AsyncClient.class);
      clients.add(client);
      factories.add(AnalyticsAcceleratorUtil.streamFactory(client, properties));
    }

    AnalyticsAcceleratorUtil.streamFactoryCache().cleanUp();

    for (int i = 0; i < OPEN_FILE_IO_COUNT; i += 1) {
      assertThat(
              AnalyticsAcceleratorUtil.streamFactoryCache()
                  .getIfPresent(Pair.of(clients.get(i), properties)))
          .as("Factory of open FileIO %s", i)
          .isSameAs(factories.get(i));
    }
  }
}

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
package org.apache.iceberg.azure;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

// this test ensures the azure bundle service registrations contain all entries across all bundled deps
// for ContextAccessor specifically, we expect entries from reactor core and reactor netty
class TestAzureBundle {

  @Test
  void shadowJar_containsAllContextAccessorServiceRegistrations() throws IOException {
    String bundlePath = System.getProperty("azure.test.bundle.jar");
    assertThat(bundlePath).as("Azure bundle path").isNotNull();

    try (JarFile bundle = new JarFile(new File(bundlePath))) {
      JarEntry serviceDescriptor = bundle.getJarEntry("META-INF/services/io.micrometer.context.ContextAccessor");
      assertThat(serviceDescriptor).as("ContextAccessor service descriptor").isNotNull();

      try (InputStream input = bundle.getInputStream(serviceDescriptor)) {
        assertThat(new String(input.readAllBytes(), UTF_8).split("\\R"))
            .contains(
                "reactor.netty.contextpropagation.ChannelContextAccessor",
                "reactor.util.context.ReactorContextAccessor");
      }
    }
  }
}

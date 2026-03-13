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
package org.apache.iceberg.connect;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to detect and configure the container runtime (Docker or Podman). */
class ContainerRuntimeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerRuntimeUtil.class);

  enum RuntimeType {
    DOCKER,
    PODMAN
  }

  enum ComposeType {
    DOCKER_COMPOSE_V2,
    DOCKER_COMPOSE_V1,
    PODMAN_COMPOSE
  }

  private ContainerRuntimeUtil() {}

  static RuntimeType detectRuntime() {
    if (isCommandAvailable("docker", "version")) {
      // Check if docker is actually podman in disguise
      if (isDockerActuallyPodman()) {
        LOG.info("Detected Podman (via docker CLI compatibility)");
        return RuntimeType.PODMAN;
      }
      LOG.info("Detected Docker runtime");
      return RuntimeType.DOCKER;
    }

    if (isCommandAvailable("podman", "version")) {
      LOG.info("Detected Podman runtime");
      return RuntimeType.PODMAN;
    }

    throw new IllegalStateException("No container runtime found. Please install Docker or Podman.");
  }

  static ComposeType detectCompose() {
    if (isCommandAvailable("docker", "compose", "version")) {
      LOG.info("Detected docker compose v2");
      return ComposeType.DOCKER_COMPOSE_V2;
    }

    if (isCommandAvailable("docker-compose", "--version")) {
      LOG.info("Detected docker-compose v1");
      return ComposeType.DOCKER_COMPOSE_V1;
    }

    if (isCommandAvailable("podman", "compose", "version")) {
      LOG.info("Detected podman compose");
      return ComposeType.PODMAN_COMPOSE;
    }

    if (isCommandAvailable("podman-compose", "--version")) {
      LOG.info("Detected podman-compose");
      return ComposeType.PODMAN_COMPOSE;
    }

    throw new IllegalStateException(
        "No compose command found. Please install docker-compose, or podman-compose.");
  }

  static String podmanSocketPath() {
    // Check DOCKER_HOST first
    String dockerHost = System.getenv("DOCKER_HOST");
    if (dockerHost != null && !dockerHost.isEmpty()) {
      return dockerHost;
    }

    // Try podman machine inspect (macOS)
    try {
      ProcessBuilder pb =
          new ProcessBuilder(
              "podman", "machine", "inspect", "--format", "{{.ConnectionInfo.PodmanSocket.Path}}");
      pb.redirectErrorStream(true);
      Process process = pb.start();
      if (process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0) {
        String socketPath;
        try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          socketPath = reader.lines().collect(Collectors.joining()).trim();
        }
        if (!socketPath.isEmpty()) {
          return "unix://" + socketPath;
        }
      }
    } catch (Exception e) {
      LOG.debug("Could not get Podman machine socket path", e);
    }

    // Try default Linux socket paths
    String uid = System.getProperty("user.name");
    try {
      ProcessBuilder pb = new ProcessBuilder("id", "-u");
      Process process = pb.start();
      if (process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0) {
        try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          uid = reader.lines().collect(Collectors.joining()).trim();
        }
      }
    } catch (Exception e) {
      LOG.debug("Could not get UID", e);
    }

    String linuxSocket = "/run/user/" + uid + "/podman/podman.sock";
    if (new java.io.File(linuxSocket).exists()) {
      return "unix://" + linuxSocket;
    }

    // Fallback: rootful Podman socket
    String rootSocket = "/run/podman/podman.sock";
    if (new java.io.File(rootSocket).exists()) {
      return "unix://" + rootSocket;
    }

    return null;
  }

  static void configurePodman() {
    String socketPath = podmanSocketPath();
    if (socketPath != null) {
      LOG.info("Configuring Testcontainers with Podman socket: {}", socketPath);
      System.setProperty("docker.host", socketPath);
    }
    // Ryuk doesn't always work well with Podman
    System.setProperty("testcontainers.ryuk.disabled", "true");
  }

  private static boolean isDockerActuallyPodman() {
    try {
      ProcessBuilder pb = new ProcessBuilder("docker", "version", "--format", "{{.Client.Name}}");
      pb.redirectErrorStream(true);
      Process process = pb.start();
      if (process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0) {
        String output;
        try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
          output = reader.lines().collect(Collectors.joining()).trim().toLowerCase();
        }
        return output.contains("podman");
      }
    } catch (Exception e) {
      LOG.debug("Could not determine if docker is podman", e);
    }
    return false;
  }

  private static boolean isCommandAvailable(String... command) {
    try {
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.redirectErrorStream(true);
      Process process = pb.start();
      // consume output to prevent blocking
      process.getInputStream().readAllBytes();
      return process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0;
    } catch (Exception e) {
      return false;
    }
  }
}

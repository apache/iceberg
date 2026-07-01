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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class AzuriteContainer extends GenericContainer<AzuriteContainer> {

  private static final Logger LOG = LoggerFactory.getLogger(AzuriteContainer.class);

  private static final int DEFAULT_PORT = 10000; // default blob service port
  private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-storage/azurite";
  private static final String DEFAULT_TAG = "3.35.0";
  private static final String LOG_WAIT_REGEX =
      "Azurite Blob service is successfully listening at .*";

  // Pulling the Azurite image from mcr.microsoft.com intermittently fails with a transient
  // ContainerFetchException/404 (registry rate-limiting or CDN inconsistency); retry the fetch a
  // few times before giving up. See https://github.com/apache/iceberg/issues/16530.
  private static final int MAX_START_ATTEMPTS = 5;
  private static final Duration START_RETRY_BASE_BACKOFF = Duration.ofSeconds(2);

  public static final String ACCOUNT = "account";
  public static final String KEY = "key";
  public static final String STORAGE_CONTAINER = "container";

  public AzuriteContainer() {
    this(DEFAULT_IMAGE + ":" + DEFAULT_TAG);
  }

  public AzuriteContainer(String image) {
    super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
    this.addExposedPort(DEFAULT_PORT);
    this.addEnv("AZURITE_ACCOUNTS", ACCOUNT + ":" + KEY);
    this.withCommand("azurite", "--blobHost", "0.0.0.0", "--skipApiVersionCheck");
    this.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(LOG_WAIT_REGEX));
  }

  @Override
  public void start() {
    startWithRetry(super::start, MAX_START_ATTEMPTS, START_RETRY_BASE_BACKOFF);
  }

  // Retries transient image-fetch failures with exponential backoff (baseBackoff doubled each
  // attempt). Exponential rather than fixed because the failure is often registry rate-limiting or
  // CDN inconsistency, which a tight fixed interval would repeatedly hit. Genuine launch failures
  // (e.g. a wait-strategy timeout) are not retried so real problems still surface immediately.
  static void startWithRetry(Runnable start, int maxAttempts, Duration baseBackoff) {
    RuntimeException lastFailure = null;
    Duration backoff = baseBackoff;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        start.run();
        return;
      } catch (ContainerFetchException | ContainerLaunchException e) {
        if (!isImageFetchFailure(e)) {
          throw e;
        }

        lastFailure = e;
        if (attempt < maxAttempts) {
          LOG.warn(
              "Failed to fetch Azurite image (attempt {}/{}), retrying in {}",
              attempt,
              maxAttempts,
              backoff,
              e);
          sleep(backoff);
          backoff = backoff.multipliedBy(2);
        }
      }
    }

    throw lastFailure;
  }

  private static boolean isImageFetchFailure(Throwable throwable) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (cause instanceof ContainerFetchException) {
        return true;
      }
    }

    return false;
  }

  private static void sleep(Duration backoff) {
    if (backoff.isZero() || backoff.isNegative()) {
      return;
    }

    try {
      Thread.sleep(backoff.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting to retry Azurite container start", e);
    }
  }

  public void createStorageContainer() {
    serviceClient().createFileSystem(STORAGE_CONTAINER);
  }

  public void deleteStorageContainer() {
    serviceClient().deleteFileSystem(STORAGE_CONTAINER);
  }

  public void createFile(String path, byte[] data) {
    try (OutputStream out = fileClient(path).getOutputStream()) {
      out.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public DataLakeServiceClient serviceClient() {
    return new DataLakeServiceClientBuilder()
        .endpoint(endpoint())
        .credential(credential())
        .buildClient();
  }

  public DataLakeFileClient fileClient(String path) {
    return new DataLakePathClientBuilder()
        .endpoint(endpoint())
        .credential(credential())
        .fileSystemName(STORAGE_CONTAINER)
        .pathName(path)
        .buildFileClient();
  }

  public String location(String path) {
    return String.format("abfs://%s@%s.dfs.core.windows.net/%s", STORAGE_CONTAINER, ACCOUNT, path);
  }

  public String endpoint() {
    return String.format("http://%s:%d/%s", getHost(), getMappedPort(DEFAULT_PORT), ACCOUNT);
  }

  public StorageSharedKeyCredential credential() {
    return new StorageSharedKeyCredential(ACCOUNT, KEY);
  }
}

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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

public class TestBaseMetastoreTableOperationsRefresh {

  private static class NoopTableOperations extends BaseMetastoreTableOperations {
    @Override
    protected String tableName() {
      return "test";
    }

    @Override
    public FileIO io() {
      return null;
    }
  }

  @Test
  public void refreshFailsFastOnForbidden() {
    NoopTableOperations ops = new NoopTableOperations();
    AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                ops.refreshFromMetadataLocation(
                    "file:/tmp/metadata.json",
                    null,
                    20,
                    location -> {
                      attempts.incrementAndGet();
                      throw new ForbiddenException("Permission denied: %s", location);
                    }))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Permission denied");

    assertThat(attempts).hasValue(1);
  }

  @Test
  public void refreshFailsFastOnNotFound() {
    NoopTableOperations ops = new NoopTableOperations();
    AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                ops.refreshFromMetadataLocation(
                    "file:/tmp/metadata.json",
                    null,
                    20,
                    location -> {
                      attempts.incrementAndGet();
                      throw new NotFoundException("File does not exist: %s", location);
                    }))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("File does not exist");

    assertThat(attempts).hasValue(1);
  }

  @Test
  public void refreshRetriesTransientFailures() {
    NoopTableOperations ops = new NoopTableOperations();
    AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                ops.refreshFromMetadataLocation(
                    "file:/tmp/metadata.json",
                    null,
                    2,
                    location -> {
                      attempts.incrementAndGet();
                      throw new RuntimeException("transient failure");
                    }))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("transient failure");

    assertThat(attempts).hasValue(3);
  }
}

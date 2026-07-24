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
package org.apache.iceberg.io;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Test {@link FailureHandler} that aggregates every reported {@link FileFailure} for inspection.
 *
 * <p>Used by FileIO bulk-delete tests to assert that the expected paths and categories were
 * reported. Thread-safe because some FileIO implementations report failures from worker threads.
 */
public class CapturingFailureHandler implements FailureHandler {
  // Thread-safe: S3 and ADLS report failures from worker threads.
  private final List<FileFailure> failures = new CopyOnWriteArrayList<>();

  @Override
  public void onFailure(FileFailure failure) {
    failures.add(failure);
  }

  public List<FileFailure> failures() {
    return Lists.newArrayList(failures);
  }

  public List<String> paths() {
    return failures().stream().map(FileFailure::path).collect(Collectors.toList());
  }

  public Map<FailureCategory, Long> categoryCounts() {
    return failures().stream()
        .collect(Collectors.groupingBy(FileFailure::category, Collectors.counting()));
  }

  public int size() {
    return failures.size();
  }
}

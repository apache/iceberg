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

package org.apache.iceberg.spark;

import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * utility class to accept thread local commit properties
 */
public class CallerWithCommitMetadata {

  private CallerWithCommitMetadata() {}

  private static final ThreadLocal<Map<String, String>> COMMIT_PROPERTIES = ThreadLocal.withInitial(ImmutableMap::of);

  public static <R> R withCommitProperties(Map<String, String> properties, Callable<R> callable)
      throws RuntimeException {
    COMMIT_PROPERTIES.set(properties);
    try {
      return callable.call();
    } catch (Exception e){
      throw new RuntimeException(e);
    } finally {
      COMMIT_PROPERTIES.set(ImmutableMap.of());
    }
  }

  public static Map<String, String> commitProperties() {
    return COMMIT_PROPERTIES.get();
  }
}

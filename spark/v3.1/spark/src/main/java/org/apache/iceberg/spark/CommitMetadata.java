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
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ExceptionUtil;

/** utility class to accept thread local commit properties */
public class CommitMetadata {

  private CommitMetadata() {}

  private static final ThreadLocal<Map<String, String>> COMMIT_PROPERTIES =
      ThreadLocal.withInitial(ImmutableMap::of);

  /**
   * running the code wrapped as a caller, and any snapshot committed within the callable object
   * will be attached with the metadata defined in properties
   *
   * @param properties extra commit metadata to attach to the snapshot committed within callable.
   *     The prefix will be removed for properties starting with {@link
   *     SnapshotSummary#EXTRA_METADATA_PREFIX}
   * @param callable the code to be executed
   * @param exClass the expected type of exception which would be thrown from callable
   */
  public static <R, E extends Exception> R withCommitProperties(
      Map<String, String> properties, Callable<R> callable, Class<E> exClass) throws E {
    Map<String, String> props = Maps.newHashMap();
    properties.forEach(
        (k, v) -> props.put(k.replace(SnapshotSummary.EXTRA_METADATA_PREFIX, ""), v));

    COMMIT_PROPERTIES.set(props);
    try {
      return callable.call();
    } catch (Throwable e) {
      ExceptionUtil.castAndThrow(e, exClass);
      return null;
    } finally {
      COMMIT_PROPERTIES.set(ImmutableMap.of());
    }
  }

  public static Map<String, String> commitProperties() {
    return COMMIT_PROPERTIES.get();
  }
}

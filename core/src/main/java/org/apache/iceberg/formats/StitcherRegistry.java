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
package org.apache.iceberg.formats;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registry of {@link StitcherBuilder}s, keyed by object model class. */
public final class StitcherRegistry {
  private StitcherRegistry() {}

  private static final Logger LOG = LoggerFactory.getLogger(StitcherRegistry.class);
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.data.GenericStitchers",
          "org.apache.iceberg.arrow.vectorized.ArrowStitchers",
          "org.apache.iceberg.flink.data.FlinkStitchers",
          "org.apache.iceberg.spark.source.SparkStitchers");

  private static final Map<Class<?>, StitcherBuilder<?>> BUILDERS = Maps.newConcurrentMap();

  static {
    registerSupportedStitchers();
  }

  /**
   * Registers a {@link StitcherBuilder}.
   *
   * @throws IllegalArgumentException if a builder is already registered for its type
   */
  public static synchronized void register(StitcherBuilder<?> builder) {
    StitcherBuilder<?> existing = BUILDERS.get(builder.type());
    Preconditions.checkArgument(
        existing == null,
        "Cannot register %s: %s is registered for type=%s",
        builder.getClass(),
        existing == null ? null : existing.getClass(),
        builder.type());

    BUILDERS.put(builder.type(), builder);
  }

  /**
   * Returns the stitcher builder registered for the given object model.
   *
   * @throws IllegalArgumentException if no builder is registered for the type
   */
  @SuppressWarnings("unchecked")
  public static <D> StitcherBuilder<D> stitcherBuilder(Class<? extends D> type) {
    StitcherBuilder<D> builder = (StitcherBuilder<D>) BUILDERS.get(type);
    Preconditions.checkArgument(
        builder != null,
        "Cannot read a data file with column files: no stitcher is registered for type %s",
        type);
    return builder;
  }

  @VisibleForTesting
  static Map<Class<?>, StitcherBuilder<?>> stitcherBuilders() {
    return BUILDERS;
  }

  private static void registerSupportedStitchers() {
    for (String classToRegister : CLASSES_TO_REGISTER) {
      register(classToRegister);
    }
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void register(String classToRegister) {
    try {
      DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
    } catch (NoSuchMethodException | NoClassDefFoundError | ExceptionInInitializerError e) {
      // the module providing the stitcher is not on the classpath
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      LOG.info(
          "Unable to call register for ({}). Check for missing jars on the classpath: {}",
          classToRegister,
          cause.toString());
    }
  }
}

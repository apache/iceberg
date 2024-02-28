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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkColumnarReaderFactoryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SparkColumnarReaderFactoryUtil.class);

  private SparkColumnarReaderFactoryUtil() {}

  public static SparkColumnarReaderFactory getSparkColumnarReaderFactory(String impl) {
    LOG.info("Loading SparkColumnarReaderFactory implementation: {}", impl);
    DynConstructors.Ctor<SparkColumnarReaderFactory> ctor;
    try {
      ctor =
          DynConstructors.builder(SparkColumnarReaderFactory.class)
              .loader(SparkColumnarReaderFactoryUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SparkColumnarReaderFactory, missing no-arg constructor: %s", impl),
          e);
    }

    SparkColumnarReaderFactory readerFactory;
    try {
      readerFactory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SparkColumnarReaderFactory, %s does not implement SparkColumnarReaderFactory.",
              impl),
          e);
    }

    return readerFactory;
  }
}

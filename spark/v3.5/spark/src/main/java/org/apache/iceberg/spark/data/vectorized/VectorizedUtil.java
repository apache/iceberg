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
package org.apache.iceberg.spark.data.vectorized;

import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedUtil {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedUtil.class);

  private VectorizedUtil() {}

  public static SparkVectorizedReaderBuilder getSparkVectorizedReaderBuilder(String impl) {
    LOG.info("Loading SparkVectorizedReaderBuilder implementation: {}", impl);
    DynConstructors.Ctor<SparkVectorizedReaderBuilder> ctor;
    try {
      ctor =
          DynConstructors.builder(SparkVectorizedReaderBuilder.class)
              .loader(VectorizedUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SparkVectorizedReaderBuilder, missing no-arg constructor: %s",
              impl),
          e);
    }

    SparkVectorizedReaderBuilder vectorizedReaderBuilder;
    try {
      vectorizedReaderBuilder = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SparkVectorizedReaderBuilder, %s does not implement SparkVectorizedReaderBuilder.",
              impl),
          e);
    }

    return vectorizedReaderBuilder;
  }

  public static BaseColumnarBatchReader getBaseColumnarBatchReader(String impl) {
    LOG.info("Loading BaseColumnarBatchReader implementation: {}", impl);
    DynConstructors.Ctor<BaseColumnarBatchReader> ctor;
    try {
      ctor =
          DynConstructors.builder(BaseColumnarBatchReader.class)
              .loader(VectorizedUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize BaseColumnarBatchReader, missing no-arg constructor: %s", impl),
          e);
    }

    BaseColumnarBatchReader baseColumnarBatchReader;
    try {
      baseColumnarBatchReader = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize BaseColumnarBatchReader, %s does not implement BaseColumnarBatchReader.",
              impl),
          e);
    }

    return baseColumnarBatchReader;
  }
}

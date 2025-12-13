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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.vectorized.VectorizedReaderBuilder;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedSparkParquetReaders {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedSparkParquetReaders.class);
  private static final String ENABLE_UNSAFE_MEMORY_ACCESS = "arrow.enable_unsafe_memory_access";
  private static final String ENABLE_UNSAFE_MEMORY_ACCESS_ENV = "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS";
  private static final String ENABLE_NULL_CHECK_FOR_GET = "arrow.enable_null_check_for_get";
  private static final String ENABLE_NULL_CHECK_FOR_GET_ENV = "ARROW_ENABLE_NULL_CHECK_FOR_GET";

  static {
    try {
      enableUnsafeMemoryAccess();
      disableNullCheckForGet();
    } catch (Exception e) {
      LOG.warn("Couldn't set Arrow properties, which may impact read performance", e);
    }
  }

  private VectorizedSparkParquetReaders() {}

  public static ColumnarBatchReader buildReader(
      Schema expectedSchema,
      MessageType fileSchema,
      Map<Integer, ?> idToConstant,
      BufferAllocator bufferAllocator) {
    return (ColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new ReaderBuilder(
                expectedSchema,
                fileSchema,
                NullCheckingForGet.NULL_CHECKING_ENABLED,
                idToConstant,
                ColumnarBatchReader::new,
                bufferAllocator));
  }

  public static ColumnarBatchReader buildReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return buildReader(expectedSchema, fileSchema, idToConstant, ArrowAllocation.rootAllocator());
  }

  public static CometColumnarBatchReader buildCometReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return (CometColumnarBatchReader)
        TypeWithSchemaVisitor.visit(
            expectedSchema.asStruct(),
            fileSchema,
            new CometVectorizedReaderBuilder(
                expectedSchema,
                fileSchema,
                idToConstant,
                readers -> new CometColumnarBatchReader(readers, expectedSchema)));
  }

  // enables unsafe memory access to avoid costly checks to see if index is within bounds
  // as long as it is not configured explicitly (see BoundsChecking in Arrow)
  private static void enableUnsafeMemoryAccess() {
    String value = confValue(ENABLE_UNSAFE_MEMORY_ACCESS, ENABLE_UNSAFE_MEMORY_ACCESS_ENV);
    if (value == null) {
      LOG.info("Enabling {}", ENABLE_UNSAFE_MEMORY_ACCESS);
      System.setProperty(ENABLE_UNSAFE_MEMORY_ACCESS, "true");
    } else {
      LOG.info("Unsafe memory access was configured explicitly: {}", value);
    }
  }

  // disables expensive null checks for every get call in favor of Iceberg nullability
  // as long as it is not configured explicitly (see NullCheckingForGet in Arrow)
  private static void disableNullCheckForGet() {
    String value = confValue(ENABLE_NULL_CHECK_FOR_GET, ENABLE_NULL_CHECK_FOR_GET_ENV);
    if (value == null) {
      LOG.info("Disabling {}", ENABLE_NULL_CHECK_FOR_GET);
      System.setProperty(ENABLE_NULL_CHECK_FOR_GET, "false");
    } else {
      LOG.info("Null checking for get calls was configured explicitly: {}", value);
    }
  }

  private static String confValue(String propName, String envName) {
    String propValue = System.getProperty(propName);
    if (propValue != null) {
      return propValue;
    }

    return System.getenv(envName);
  }

  private static class ReaderBuilder extends VectorizedReaderBuilder {

    ReaderBuilder(
        Schema expectedSchema,
        MessageType parquetSchema,
        boolean setArrowValidityVector,
        Map<Integer, ?> idToConstant,
        Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
        BufferAllocator bufferAllocator) {
      super(
          expectedSchema,
          parquetSchema,
          setArrowValidityVector,
          idToConstant,
          readerFactory,
          SparkUtil::internalToSpark,
          bufferAllocator);
    }
  }
}

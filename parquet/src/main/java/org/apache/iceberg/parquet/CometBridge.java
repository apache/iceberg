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
package org.apache.iceberg.parquet;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.column.ColumnDescriptor;

/**
 * Bridge class that uses reflection to interact with Comet FileReader without adding a direct
 * dependency on the comet-spark jar.
 */
class CometBridge {
  private static final String FILE_READER_CLASS = "org.apache.comet.parquet.FileReader";
  private static final String WRAPPED_INPUT_FILE_CLASS =
      "org.apache.comet.parquet.WrappedInputFile";
  private static final String READ_OPTIONS_CLASS = "org.apache.comet.parquet.ReadOptions";
  private static final String READ_OPTIONS_BUILDER_CLASS =
      "org.apache.comet.parquet.ReadOptions$Builder";
  private static final String ROW_GROUP_READER_CLASS = "org.apache.comet.parquet.RowGroupReader";

  private static volatile Boolean cometAvailable = null;
  private static Class<?> fileReaderClass;
  private static Class<?> wrappedInputFileClass;
  private static Class<?> readOptionsClass;
  private static Class<?> readOptionsBuilderClass;
  private static Class<?> rowGroupReaderClass;

  private static Constructor<?> fileReaderConstructor;
  private static Constructor<?> wrappedInputFileConstructor;
  private static Constructor<?> readOptionsBuilderConstructor;
  private static Method readOptionsBuildMethod;
  private static Method setRequestedSchemaFromSpecsMethod;
  private static Method readNextRowGroupMethod;
  private static Method skipNextRowGroupMethod;
  private static Method closeMethod;
  private static Method getRowCountMethod;

  // prevent instantiation.
  private CometBridge() {}

  /**
   * Checks if Comet classes are available in the classpath.
   *
   * @return true if Comet is available, false otherwise
   */
  public static boolean isCometAvailable() {
    if (cometAvailable == null) {
      synchronized (CometBridge.class) {
        if (cometAvailable == null) {
          try {
            initializeClasses();
            cometAvailable = true;
          } catch (Exception e) {
            cometAvailable = false;
          }
        }
      }
    }
    return cometAvailable;
  }

  private static void initializeClasses() throws Exception {
    // Load classes
    fileReaderClass = Class.forName(FILE_READER_CLASS);
    wrappedInputFileClass = Class.forName(WRAPPED_INPUT_FILE_CLASS);
    readOptionsClass = Class.forName(READ_OPTIONS_CLASS);
    readOptionsBuilderClass = Class.forName(READ_OPTIONS_BUILDER_CLASS);
    rowGroupReaderClass = Class.forName(ROW_GROUP_READER_CLASS);

    // Initialize constructors and methods
    fileReaderConstructor =
        fileReaderClass.getConstructor(
            wrappedInputFileClass,
            readOptionsClass,
            Map.class,
            Long.class,
            Long.class,
            byte[].class,
            byte[].class);

    // WrappedInputFile constructor takes Object parameter
    wrappedInputFileConstructor = wrappedInputFileClass.getConstructor(Object.class);

    // ReadOptionsBuilder constructor takes Configuration parameter
    readOptionsBuilderConstructor =
        readOptionsBuilderClass.getConstructor(org.apache.hadoop.conf.Configuration.class);
    readOptionsBuildMethod = readOptionsBuilderClass.getMethod("build");

    setRequestedSchemaFromSpecsMethod =
        fileReaderClass.getMethod("setRequestedSchemaFromSpecs", List.class);
    readNextRowGroupMethod = fileReaderClass.getMethod("readNextRowGroup");
    skipNextRowGroupMethod = fileReaderClass.getMethod("skipNextRowGroup");
    closeMethod = fileReaderClass.getMethod("close");

    getRowCountMethod = rowGroupReaderClass.getMethod("getRowCount");
  }

  /** Wrapper for Comet FileReader that uses reflection. */
  public static class FileReaderWrapper implements AutoCloseable {
    private final Object fileReader;

    private FileReaderWrapper(Object fileReader) {
      this.fileReader = fileReader;
    }

    /** Creates a new FileReader instance using reflection. */
    public static FileReaderWrapper create(
        InputFile file,
        Object readOptions,
        Map<String, String> properties,
        Long start,
        Long length,
        byte[] fileEncryptionKey,
        byte[] fileAADPrefix)
        throws Exception {

      // Create WrappedInputFile
      Object wrappedInputFile = wrappedInputFileConstructor.newInstance(file);

      // Create FileReader
      Object fileReader =
          fileReaderConstructor.newInstance(
              wrappedInputFile,
              readOptions,
              properties,
              start,
              length,
              fileEncryptionKey,
              fileAADPrefix);

      return new FileReaderWrapper(fileReader);
    }

    /** Sets the requested schema from ParquetColumnSpec list. */
    public void setRequestedSchemaFromSpecs(List<Object> specs) throws Exception {
      setRequestedSchemaFromSpecsMethod.invoke(fileReader, specs);
    }

    /** Reads the next row group. */
    public RowGroupReaderWrapper readNextRowGroup() throws Exception {
      Object rowGroupReader = readNextRowGroupMethod.invoke(fileReader);
      return new RowGroupReaderWrapper(rowGroupReader);
    }

    /** Skips the next row group. */
    public void skipNextRowGroup() throws Exception {
      skipNextRowGroupMethod.invoke(fileReader);
    }

    /** Closes the file reader. */
    public void close() throws Exception {
      closeMethod.invoke(fileReader);
    }
  }

  /** Wrapper for Comet RowGroupReader that uses reflection. */
  public static class RowGroupReaderWrapper {
    private final Object rowGroupReader;

    private RowGroupReaderWrapper(Object rowGroupReader) {
      this.rowGroupReader = rowGroupReader;
    }

    /** Gets the row count for this row group. */
    public long getRowCount() throws Exception {
      return (Long) getRowCountMethod.invoke(rowGroupReader);
    }

    /** Returns the underlying row group reader object. */
    public Object getRowGroupReader() {
      return rowGroupReader;
    }
  }

  /** Creates ReadOptions using reflection. */
  public static Object createReadOptions(org.apache.hadoop.conf.Configuration conf)
      throws Exception {
    if (!isCometAvailable()) {
      throw new IllegalStateException("Comet is not available in the classpath");
    }

    Object builder = readOptionsBuilderConstructor.newInstance(conf);
    return readOptionsBuildMethod.invoke(builder);
  }

  /** Creates ParquetColumnSpec from ColumnDescriptor using CometTypeUtils. */
  public static Object createParquetColumnSpec(ColumnDescriptor descriptor) throws Exception {
    if (!isCometAvailable()) {
      throw new IllegalStateException("Comet is not available in the classpath");
    }

    return CometTypeUtils.descriptorToParquetColumnSpec(descriptor);
  }
}

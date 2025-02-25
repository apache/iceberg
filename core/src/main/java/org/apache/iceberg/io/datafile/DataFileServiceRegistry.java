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
package org.apache.iceberg.io.datafile;

import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which maintains the available {@link ReadBuilder}s and {@link WriteBuilder}s. Based on
 * the `file format`, the required `data type` and the reader/writer `builderType` the registry
 * returns the correct reader and writer builders. These services could be used to generate the
 * correct readers and writers.
 */
public final class DataFileServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileServiceRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final String[] CLASSES_TO_REGISTER =
      new String[] {
        "org.apache.iceberg.parquet.Parquet",
        "org.apache.iceberg.orc.ORC",
        "org.apache.iceberg.arrow.vectorized.ArrowReader",
        "org.apache.iceberg.flink.source.RowDataFileScanTaskReader",
        "org.apache.iceberg.flink.sink.FlinkAppenderFactory",
        "org.apache.iceberg.spark.source.DataFileServices"
      };

  private static final Map<Key, Function<EncryptedOutputFile, WriteBuilder<?, ?>>> WRITE_BUILDERS =
      Maps.newConcurrentMap();
  private static final Map<Key, Function<InputFile, ReadBuilder<?, ?>>> READ_BUILDERS =
      Maps.newConcurrentMap();

  /** Registers a new writer builder for the given format/input type. */
  public static void registerWrite(
      FileFormat format,
      String inputType,
      Function<EncryptedOutputFile, WriteBuilder<?, ?>> writeBuilder) {
    Key key = new Key(format, inputType, null);
    if (WRITE_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Write builder %s clashes with %s. Both serves %s",
              writeBuilder.getClass(), WRITE_BUILDERS.get(key), key));
    }

    WRITE_BUILDERS.putIfAbsent(key, writeBuilder);
  }

  /** Registers a new reader builder for the given format/input type. */
  public static void registerRead(
      FileFormat format, String outputType, Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    registerRead(format, outputType, null, readBuilder);
  }

  /** Registers a new reader builder for the given format/input type/reader type. */
  public static void registerRead(
      FileFormat format,
      String outputType,
      String readerType,
      Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    Key key = new Key(format, outputType, readerType);
    if (READ_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Read builder %s clashes with %s. Both serves %s",
              readBuilder.getClass(), READ_BUILDERS.get(key), key));
    }

    READ_BUILDERS.put(new Key(format, outputType, readerType), readBuilder);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    registerRead(
        FileFormat.AVRO,
        Record.class.getName(),
        inputFile ->
            new Avro.DataReadBuilder<>(inputFile).readerFunction(PlannedDataReader::create));

    registerWrite(
        FileFormat.AVRO,
        Record.class.getName(),
        outputFile ->
            new Avro.AvroDataWriteBuilder<Record, Schema>(outputFile)
                .writerFunction((nativeType, schema) -> DataWriter.create(schema))
                .positionDeleteWriterFunction((nativeType, schema) -> DataWriter.create(schema)));

    // Uses dynamic methods to call the `register` for the listed classes
    for (String s : CLASSES_TO_REGISTER) {
      try {
        DynMethods.StaticMethod register =
            DynMethods.builder("register").impl(s).buildStaticChecked();

        register.invoke();

      } catch (NoSuchMethodException e) {
        // failing to register readers/writers is normal and does not require a stack trace
        LOG.info("Unable to register {} for data files: {}", s, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  private DataFileServiceRegistry() {}

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <D, F> ReadBuilder<D, F> readerBuilder(
      FileFormat format, String returnType, InputFile inputFile) {
    return readerBuilder(format, returnType, null, inputFile);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param builderType of the reader builder
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <D, F> ReadBuilder<D, F> readerBuilder(
      FileFormat format, String returnType, String builderType, InputFile inputFile) {
    return (ReadBuilder<D, F>)
        READ_BUILDERS.get(new Key(format, returnType, builderType)).apply(inputFile);
  }

  /**
   * Provides a writer for the given output file which writes objects with a given inputType.
   *
   * @param format of the file to read
   * @param inputType accepted by the writer
   * @param outputFile to write
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <D, T> WriteBuilder<D, T> writeBuilder(
      FileFormat format, String inputType, EncryptedOutputFile outputFile) {
    return (WriteBuilder<D, T>)
        WRITE_BUILDERS.get(new Key(format, inputType, null)).apply(outputFile);
  }

  /** Key used to identify readers and writers in the {@link DataFileServiceRegistry}. */
  private static class Key {
    private final FileFormat fileFormat;
    private final String dataType;
    private final String builderType;

    /**
     * Create the key when there are multiple builder types available.
     *
     * @param fileFormat the service handles
     * @param dataType the service handles
     * @param builderType identifier of the builder type when multiple readers/writers are available
     */
    private Key(FileFormat fileFormat, String dataType, String builderType) {
      this.fileFormat = fileFormat;
      this.dataType = dataType;
      this.builderType = builderType;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileFormat", fileFormat)
          .add("dataType", dataType)
          .add("builderType", builderType)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Key)) {
        return false;
      }

      Key other = (Key) o;
      return Objects.equal(other.fileFormat, fileFormat)
          && Objects.equal(other.dataType, dataType)
          && java.util.Objects.equals(other.builderType, builderType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fileFormat, dataType, builderType);
    }
  }
}

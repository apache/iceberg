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
package org.apache.iceberg.data;

import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAccessFactory;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.WriteBuilder;

/**
 * A factory to generate file-format-specific readers and writers through a unified interface. The
 * appropriate builder is selected based on {@link FileFormat}.
 *
 * <p>Read builders are returned directly from the factory. Write builders may be wrapped in
 * specialized content file writer implementations depending on the requested builder type.
 */
public final class FileAccessor<E, D, V> {
  private final Map<FileFormat, FileAccessFactory<E, D, V>> factories;

  /**
   * Constructs a FileAccessor with the provided factories for each file format.
   *
   * @param factories a map of file formats to their corresponding access factories
   */
  public FileAccessor(Map<FileFormat, FileAccessFactory<E, D, V>> factories) {
    this.factories = factories;
  }

  /**
   * Returns a reader builder for the specified file format.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param inputFile source file to read data from
   * @return a configured reader builder for the specified format
   */
  public ReadBuilder<?, D> readBuilder(FileFormat format, InputFile inputFile) {
    return factories.get(format).readBuilder(inputFile);
  }

  /**
   * Returns a vectorized reader builder for the specified file format.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param inputFile source file to read data from
   * @return a configured reader builder for the specified format
   */
  public ReadBuilder<?, V> vectorizedReadBuilder(FileFormat format, InputFile inputFile) {
    return factories.get(format).vectorizedReadBuilder(inputFile);
  }

  /**
   * Returns a writer builder for appending data to the specified output file.
   *
   * <p>The returned builder produces a {@link FileAppender} that accepts data records and persists
   * them using the given file format. Data is written to the output file, but this basic writer
   * does not collect or return {@link ContentFile} metadata.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data
   * @return a configured writer builder for creating the appender
   */
  public WriteBuilder<?, E, D> writeBuilder(FileFormat format, EncryptedOutputFile outputFile) {
    return factories.get(format).dataWriteBuilder(outputFile.encryptingOutputFile());
  }

  /**
   * Returns a writer builder for generating a {@link DataFile}.
   *
   * <p>The returned builder produces a writer that accepts data records and persists them using the
   * provided file format. Unlike basic writers, this writer collects file metadata during the
   * writing process and generates a {@link DataFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public DataWriteBuilder<?, E, D> dataWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    WriteBuilder<?, E, D> writeBuilder =
        factories.get(format).equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forDataFile(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with equality deletes.
   *
   * <p>The returned builder produces a writer that accepts data records and persists them using the
   * given file format. The writer persists equality delete records that identify rows to be deleted
   * based on the configured equality fields, producing a {@link DeleteFile} that can be used for
   * table operations.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public EqualityDeleteWriteBuilder<?, E, D> equalityDeleteWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    WriteBuilder<?, E, D> writeBuilder =
        factories.get(format).equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forEqualityDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with position-based deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer accepts {@link PositionDelete}
   * records that identify rows to be deleted by file path and position, producing a {@link
   * DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data will accept
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public PositionDeleteWriteBuilder<?, E, D> positionDeleteWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    WriteBuilder<?, E, PositionDelete<D>> writeBuilder =
        factories.get(format).positionDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forPositionDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }
}

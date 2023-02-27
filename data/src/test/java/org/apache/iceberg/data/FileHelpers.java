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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;

public class FileHelpers {
  private FileHelpers() {}

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(
      Table table, OutputFile out, List<Pair<CharSequence, Long>> deletes) throws IOException {
    return writeDeleteFile(table, out, null, deletes);
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(
      Table table, OutputFile out, StructLike partition, List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    FileAppenderFactory<Record> factory = new GenericAppenderFactory(table.schema(), table.spec());

    PositionDeleteWriter<Record> writer =
        factory.newPosDeleteWriter(encrypt(out), format, partition);
    PositionDelete<Record> posDelete = PositionDelete.create();
    try (Closeable toClose = writer) {
      for (Pair<CharSequence, Long> delete : deletes) {
        writer.write(posDelete.set(delete.first(), delete.second(), null));
      }
    }

    return Pair.of(writer.toDeleteFile(), writer.referencedDataFiles());
  }

  public static DeleteFile writeDeleteFile(
      Table table, OutputFile out, List<Record> deletes, Schema deleteRowSchema)
      throws IOException {
    return writeDeleteFile(table, out, null, deletes, deleteRowSchema);
  }

  public static DeleteFile writeDeleteFile(
      Table table,
      OutputFile out,
      StructLike partition,
      List<Record> deletes,
      Schema deleteRowSchema)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    int[] equalityFieldIds =
        deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();
    FileAppenderFactory<Record> factory =
        new GenericAppenderFactory(
            table.schema(), table.spec(), equalityFieldIds, deleteRowSchema, null);

    EqualityDeleteWriter<Record> writer =
        factory.newEqDeleteWriter(encrypt(out), format, partition);
    try (Closeable toClose = writer) {
      writer.write(deletes);
    }

    return writer.toDeleteFile();
  }

  public static DataFile writeDataFile(Table table, OutputFile out, List<Record> rows)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericAppenderFactory factory = new GenericAppenderFactory(table.schema());

    FileAppender<Record> writer = factory.newAppender(out, format);
    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  public static DataFile writeDataFile(
      Table table, OutputFile out, StructLike partition, List<Record> rows) throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());

    FileAppender<Record> writer = factory.newAppender(out, format);
    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(out.location())
        .withPartition(partition)
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  public static DeleteFile writePosDeleteFile(
      Table table, OutputFile out, StructLike partition, List<PositionDelete<?>> deletes)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    FileAppenderFactory<Record> factory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            null, // Equality Fields
            null, // Equality Delete row schema
            table.schema()); // Position Delete row schema (will be wrapped)

    PositionDeleteWriter<?> writer = factory.newPosDeleteWriter(encrypt(out), format, partition);
    try (Closeable toClose = writer) {
      for (PositionDelete delete : deletes) {
        writer.write(delete);
      }
    }

    return writer.toDeleteFile();
  }

  private static EncryptedOutputFile encrypt(OutputFile out) {
    return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
  }

  private static FileFormat defaultFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.fromString(formatString);
  }
}

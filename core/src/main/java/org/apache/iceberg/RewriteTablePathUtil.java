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
package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;

public class RewriteTablePathUtil {

  public static List<Pair<String, String>> rewriteManifest(
      FileIO io,
      int format,
      PartitionSpec spec,
      OutputFile outputFile,
      ManifestFile manifestFile,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix)
      throws IOException {
    try (ManifestWriter<DataFile> writer =
            ManifestFiles.write(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifestFile, io, specsById).select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(entry -> newDataFile(entry, spec, sourcePrefix, targetPrefix, writer))
          .collect(Collectors.toList());
    }
  }

  public static List<Pair<String, String>> rewriteDeleteManifest(
      FileIO io,
      int format,
      PartitionSpec spec,
      OutputFile outputFile,
      ManifestFile manifestFile,
      Map<Integer, PartitionSpec> specsById,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation,
      PositionDeleteReaderWriter positionDeleteReaderWriter)
      throws IOException {
    try (ManifestWriter<DeleteFile> writer =
            ManifestFiles.writeDeleteManifest(format, spec, outputFile, manifestFile.snapshotId());
        ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifestFile, io, specsById)
                .select(Arrays.asList("*"))) {
      return StreamSupport.stream(reader.entries().spliterator(), false)
          .map(
              entry -> {
                try {
                  return newDeleteFile(
                      entry,
                      io,
                      spec,
                      sourcePrefix,
                      targetPrefix,
                      stagingLocation,
                      writer,
                      positionDeleteReaderWriter);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toList());
    }
  }

  private static Pair<String, String> newDataFile(
      ManifestEntry<DataFile> entry,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      ManifestWriter<DataFile> writer) {
    DataFile dataFile = entry.file();
    String sourceDataFilePath = dataFile.location();
    Preconditions.checkArgument(
        sourceDataFilePath.startsWith(sourcePrefix),
        "Encountered data file %s not under the source prefix %s",
        sourceDataFilePath,
        sourcePrefix);
    String targetDataFilePath = newPath(sourceDataFilePath, sourcePrefix, targetPrefix);
    DataFile newDataFile =
        DataFiles.builder(spec).copy(entry.file()).withPath(targetDataFilePath).build();
    appendEntryWithFile(entry, writer, newDataFile);
    return Pair.of(sourceDataFilePath, newDataFile.location());
  }

  private static Pair<String, String> newDeleteFile(
      ManifestEntry<DeleteFile> entry,
      FileIO io,
      PartitionSpec spec,
      String sourcePrefix,
      String targetPrefix,
      String stagingLocation,
      ManifestWriter<DeleteFile> writer,
      PositionDeleteReaderWriter posDeleteReaderWriter)
      throws IOException {

    DeleteFile file = entry.file();

    switch (file.content()) {
      case POSITION_DELETES:
        DeleteFile posDeleteFile =
            rewritePositionDeleteFile(
                io, file, spec, sourcePrefix, stagingLocation, targetPrefix, posDeleteReaderWriter);
        String targetDeleteFilePath = newPath(file.location(), sourcePrefix, targetPrefix);
        DeleteFile movedFile =
            FileMetadata.deleteFileBuilder(spec)
                .copy(posDeleteFile)
                .withPath(targetDeleteFilePath)
                .build();
        appendEntryWithFile(entry, writer, movedFile);
        return Pair.of(posDeleteFile.location(), movedFile.location());
      case EQUALITY_DELETES:
        DeleteFile eqDeleteFile = newEqualityDeleteFile(file, spec, sourcePrefix, targetPrefix);
        appendEntryWithFile(entry, writer, eqDeleteFile);
        return Pair.of(file.location(), eqDeleteFile.location());
      default:
        throw new UnsupportedOperationException("Unsupported delete file type: " + file.content());
    }
  }

  private static <F extends ContentFile<F>> void appendEntryWithFile(
      ManifestEntry<F> entry, ManifestWriter<F> writer, F file) {

    switch (entry.status()) {
      case ADDED:
        writer.add(file);
        break;
      case EXISTING:
        writer.existing(
            file, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
      case DELETED:
        writer.delete(file, entry.dataSequenceNumber(), entry.fileSequenceNumber());
        break;
    }
  }

  public interface PositionDeleteReaderWriter {
    CloseableIterable<Record> reader(InputFile inputFile, FileFormat format, PartitionSpec spec);

    PositionDeleteWriter<Record> writer(
        OutputFile outputFile,
        FileFormat format,
        PartitionSpec spec,
        StructLike partition,
        Schema rowSchema)
        throws IOException;
  }

  private static DeleteFile rewritePositionDeleteFile(
      FileIO io,
      DeleteFile current,
      PartitionSpec spec,
      String sourcePrefix,
      String stagingLocation,
      String targetPrefix,
      PositionDeleteReaderWriter posDeleteReaderWriter)
      throws IOException {
    String path = current.location();
    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: "
              + sourcePrefix
              + " but was "
              + path);
    }
    String newPath = stagingPath(path, stagingLocation);

    OutputFile targetFile = io.newOutputFile(newPath);
    InputFile sourceFile = io.newInputFile(path);

    try (CloseableIterable<org.apache.iceberg.data.Record> reader =
        posDeleteReaderWriter.reader(sourceFile, current.format(), spec)) {
      org.apache.iceberg.data.Record record = null;
      Schema rowSchema = null;
      CloseableIterator<org.apache.iceberg.data.Record> recordIt = reader.iterator();

      if (recordIt.hasNext()) {
        record = recordIt.next();
        rowSchema = record.get(2) != null ? spec.schema() : null;
      }

      PositionDeleteWriter<Record> writer =
          posDeleteReaderWriter.writer(
              targetFile, current.format(), spec, current.partition(), rowSchema);

      try (writer) {
        if (record != null) {
          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
        }

        while (recordIt.hasNext()) {
          record = recordIt.next();
          writer.write(newPositionDeleteRecord(record, sourcePrefix, targetPrefix));
        }
      }
      return writer.toDeleteFile();
    }
  }

  private static PositionDelete newPositionDeleteRecord(
      Record record, String sourcePrefix, String targetPrefix) {
    PositionDelete delete = PositionDelete.create();
    String oldPath = (String) record.get(0);
    String newPath = oldPath;
    if (oldPath.startsWith(sourcePrefix)) {
      newPath = newPath(oldPath, sourcePrefix, targetPrefix);
    }
    delete.set(newPath, (Long) record.get(1), record.get(2));
    return delete;
  }

  private static DeleteFile newEqualityDeleteFile(
      DeleteFile file, PartitionSpec spec, String sourcePrefix, String targetPrefix) {
    String path = file.location();

    if (!path.startsWith(sourcePrefix)) {
      throw new UnsupportedOperationException(
          "Expected delete file to be under the source prefix: "
              + sourcePrefix
              + " but was "
              + path);
    }
    int[] equalityFieldIds = file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray();
    String newPath = newPath(path, sourcePrefix, targetPrefix);
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldIds)
        .copy(file)
        .withPath(newPath)
        .withSplitOffsets(file.splitOffsets())
        .build();
  }

  private static String newPath(String path, String sourcePrefix, String targetPrefix) {
    return combinePaths(targetPrefix, relativize(path, sourcePrefix));
  }

  private static String combinePaths(String absolutePath, String relativePath) {
    String combined = absolutePath;
    if (!combined.endsWith("/")) {
      combined += "/";
    }
    combined += relativePath;
    return combined;
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private static String relativize(String path, String prefix) {
    String toRemove = prefix;
    if (!toRemove.endsWith("/")) {
      toRemove += "/";
    }
    if (!path.startsWith(toRemove)) {
      throw new IllegalArgumentException(
          String.format("Path %s does not start with %s", path, toRemove));
    }
    return path.substring(toRemove.length());
  }

  private static String stagingPath(String originalPath, String stagingLocation) {
    return stagingLocation + fileName(originalPath);
  }
}

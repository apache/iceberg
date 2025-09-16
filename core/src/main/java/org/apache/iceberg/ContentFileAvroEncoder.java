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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;

/**
 * A utility class to encode {@link ContentFile} implementations as Avro in a backwards compatible
 * way. It uses the same Avro encoding mechanism as {@link ManifestWriter} and {@link
 * ManifestReader}. *
 */
public class ContentFileAvroEncoder {
  private ContentFileAvroEncoder() {}

  public static <T> byte[] encode(ContentFile<T>[] files) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream view = new DataOutputStream(out);

    Map<Types.StructType, List<ContentFile<T>>> filesByPartitionType = Maps.newLinkedHashMap();
    for (ContentFile<T> dataFile : files) {
      Types.StructType partitionType = ((PartitionData) dataFile.partition()).getPartitionType();
      filesByPartitionType
          .computeIfAbsent(partitionType, ignoredSpec -> Lists.newArrayList())
          .add(dataFile);
    }
    // Number of unique partition types
    view.writeInt(filesByPartitionType.size());

    for (Map.Entry<Types.StructType, List<ContentFile<T>>> entry :
        filesByPartitionType.entrySet()) {
      Types.StructType partitionType = entry.getKey();
      List<ContentFile<T>> dataFiles = entry.getValue();
      Schema fileSchema = new Schema(DataFile.getType(partitionType).fields());

      String partitionSchema = SchemaParser.toJson(partitionType.asSchema());
      view.writeUTF(partitionSchema);

      InMemoryOutputFile outputFile = new InMemoryOutputFile();
      try (FileAppender<ContentFile<T>> fileAppender =
          InternalData.write(FileFormat.AVRO, outputFile).schema(fileSchema).build()) {
        fileAppender.addAll(dataFiles);
      }

      byte[] serialisedFiles = outputFile.toByteArray();
      view.writeInt(serialisedFiles.length);
      view.write(serialisedFiles);
    }

    return out.toByteArray();
  }

  public static DataFile[] decodeDataFiles(byte[] serialized) throws IOException {
    return decode(serialized, GenericDataFile.class);
  }

  public static DeleteFile[] decodeDeleteFiles(byte[] serialized) throws IOException {
    return decode(serialized, GenericDeleteFile.class);
  }

  private static <T extends StructLike> T[] decode(byte[] serialized, Class<T> fileClass)
      throws IOException {
    DataInputStream view = new DataInputStream(new ByteArrayInputStream(serialized));
    List<T> files = Lists.newArrayList();

    int uniqueSpecTypes = view.readInt();
    for (int i = 0; i < uniqueSpecTypes; i++) {
      Schema partitionSchema = SchemaParser.fromJson(view.readUTF());
      Schema fileSchema = new Schema(DataFile.getType(partitionSchema.asStruct()).fields());

      byte[] fileBuffer = new byte[view.readInt()];
      ByteStreams.readFully(view, fileBuffer);

      try (CloseableIterable<T> reader =
          InternalData.read(FileFormat.AVRO, new InMemoryInputFile(fileBuffer))
              .project(fileSchema)
              .setRootType(fileClass)
              .setCustomType(DataFile.PARTITION_ID, PartitionData.class)
              .build()) {
        reader.forEach(files::add);
      }
    }

    return files.toArray((T[]) Array.newInstance(fileClass, files.size()));
  }
}

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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.JsonUtil;


public class NamespaceMetadataParser {
  public enum Codec {
    NONE(""),
    GZIP(".gz");

    private final String extension;

    Codec(String extension) {
      this.extension = extension;
    }

    public static NamespaceMetadataParser.Codec fromName(String codecName) {
      Preconditions.checkArgument(codecName != null, "Codec name is null");
      return NamespaceMetadataParser.Codec.valueOf(codecName.toUpperCase(Locale.ENGLISH));
    }

    public static NamespaceMetadataParser.Codec fromFileName(String fileName) {
      Preconditions.checkArgument(fileName.contains(".namespace_metadata.json"),
          "%s is not a valid metadata file", fileName);
      // we have to be backward-compatible with .metadata.json.gz files
      if (fileName.endsWith(".namespace_metadata.json.gz")) {
        return NamespaceMetadataParser.Codec.GZIP;
      }
      String fileNameWithoutSuffix = fileName.substring(0, fileName.lastIndexOf(".namespace_metadata.json"));
      if (fileNameWithoutSuffix.endsWith(NamespaceMetadataParser.Codec.GZIP.extension)) {
        return NamespaceMetadataParser.Codec.GZIP;
      } else {
        return NamespaceMetadataParser.Codec.NONE;
      }
    }
  }

  private NamespaceMetadataParser(){}

  // visible for testing
  private static final String FORMAT_VERSION = "format-version";
  private static final String UUID = "uuid";
  private static final String LOCATION = "location";
  private static final String LAST_UPDATED_MILLIS = "last-updated-ms";
  private static final String NAMESPACES = "namespaces";


  public static void overwrite(NamespaceMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(NamespaceMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  private  static void internalWrite(
      NamespaceMetadata metadata, OutputFile outputFile, boolean overwrite) {
    boolean isGzip = NamespaceMetadataParser.Codec.fromFileName(
        outputFile.location()) == NamespaceMetadataParser.Codec.GZIP;
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer = new OutputStreamWriter(isGzip ? new GZIPOutputStream(stream) : stream)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json to file: %s", outputFile);
    }
  }

  public static String getFileExtension(Codec codec) {
    return codec.extension + ".namespace_metadata.json";
  }

  public static String getOldFileExtension(Codec codec) {
    // we have to be backward-compatible with .metadata.json.gz files
    return ".namespace_metadata.json" + codec.extension;
  }


  public static String toJson(NamespaceMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", metadata);
    }
    return writer.toString();
  }

  private static void toJson(NamespaceMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, NamespaceMetadata.NAMESPACE_FORMAT_VERSION);
    generator.writeStringField(UUID, metadata.uuid());
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
    generator.writeArrayFieldStart(NAMESPACES);
    for (Namespace namespace : metadata.namespaces()) {
      namespace.setTimestampMillis(metadata.lastUpdatedMillis());
      NamespaceParser.toJson(namespace, generator);
    }
    generator.writeEndArray();
    generator.writeEndObject();

  }

  public static NamespaceMetadata read(FileIO io, InputFile file) {
    Codec codec = Codec.fromFileName(file.location());
    try (InputStream is = codec == Codec.GZIP ? new GZIPInputStream(file.newStream()) : file.newStream()) {
      return fromJson(io, file, JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read file: %s", file);
    }
  }

  private static NamespaceMetadata fromJson(FileIO io, InputFile file, JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(formatVersion == NamespaceMetadata.NAMESPACE_FORMAT_VERSION,
        "Cannot read unsupported version %s", formatVersion);

    String uuid = JsonUtil.getStringOrNull(UUID, node);
    String location = JsonUtil.getString(LOCATION, node);
    long lastUpdatedMillis = JsonUtil.getLong(LAST_UPDATED_MILLIS, node);


    JsonNode namespaceArray = node.get(NAMESPACES);
    Preconditions.checkArgument(namespaceArray.isArray(),
        "Cannot parse snapshots from non-array: %s", namespaceArray);

    List<Namespace> namespaces = Lists.newArrayListWithExpectedSize(namespaceArray.size());
    Iterator<JsonNode> iterator = namespaceArray.elements();
    while (iterator.hasNext()) {
      namespaces.add(NamespaceParser.fromJson(io, iterator.next()));
    }
    return new NamespaceMetadata(file, uuid, location, lastUpdatedMillis, namespaces);
  }
}

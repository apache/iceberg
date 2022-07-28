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
package org.apache.iceberg.puffin;

import java.util.Map;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Utility class for reading and writing Puffin files. */
public final class Puffin {
  private Puffin() {}

  public static WriteBuilder write(OutputFile outputFile) {
    return new WriteBuilder(outputFile);
  }

  /** A builder for {@link PuffinWriter}. */
  public static class WriteBuilder {
    private final OutputFile outputFile;
    private final Map<String, String> properties = Maps.newLinkedHashMap();
    private boolean compressFooter = false;
    private PuffinCompressionCodec defaultBlobCompression = PuffinCompressionCodec.NONE;

    private WriteBuilder(OutputFile outputFile) {
      this.outputFile = outputFile;
    }

    /** Sets file-level property to be written */
    public WriteBuilder set(String property, String value) {
      properties.put(property, value);
      return this;
    }

    /** Sets file-level properties to be written */
    public WriteBuilder setAll(Map<String, String> props) {
      this.properties.putAll(props);
      return this;
    }

    /** Sets file-level {@value StandardPuffinProperties#CREATED_BY_PROPERTY} property. */
    public WriteBuilder createdBy(String applicationIdentifier) {
      this.properties.put(StandardPuffinProperties.CREATED_BY_PROPERTY, applicationIdentifier);
      return this;
    }

    /** Configures the writer to compress the footer. */
    public WriteBuilder compressFooter() {
      this.compressFooter = true;
      return this;
    }

    /**
     * Configures the writer to compress the blobs. Can be overwritten by {@link Blob} attribute.
     */
    public WriteBuilder compressBlobs(PuffinCompressionCodec compression) {
      this.defaultBlobCompression = compression;
      return this;
    }

    public PuffinWriter build() {
      return new PuffinWriter(outputFile, properties, compressFooter, defaultBlobCompression);
    }
  }

  public static ReadBuilder read(InputFile inputFile) {
    return new ReadBuilder(inputFile);
  }

  /** A builder for {@link PuffinReader}. */
  public static final class ReadBuilder {
    private final InputFile inputFile;
    private Long fileSize;
    private Long footerSize;

    private ReadBuilder(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    /** Passes known file size to the reader. This may improve read performance. */
    public ReadBuilder withFileSize(long size) {
      this.fileSize = size;
      return this;
    }

    /** Passes known footer size to the reader. This may improve read performance. */
    public ReadBuilder withFooterSize(long size) {
      this.footerSize = size;
      return this;
    }

    public PuffinReader build() {
      return new PuffinReader(inputFile, fileSize, footerSize);
    }
  }
}

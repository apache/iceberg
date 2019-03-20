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

import org.apache.iceberg.types.Comparators;

/**
 * Enum of supported file formats.
 */
public enum FileFormat {
  ORC("orc", true),
  PARQUET("parquet", true),
  AVRO("avro", true);

  private final String ext;
  private final boolean splittable;

  FileFormat(String ext, boolean splittable) {
    this.ext = "." + ext;
    this.splittable = splittable;
  }

  public boolean isSplittable() {
    return splittable;
  }

  /**
   * Returns filename with this format's extension added, if necessary.
   *
   * @param filename a filename or path
   * @return if the ext is present, the filename, otherwise the filename with ext added
   */
  public String addExtension(String filename) {
    if (filename.endsWith(ext)) {
      return filename;
    }
    return filename + ext;
  }

  public static FileFormat fromFileName(CharSequence filename) {
    int lastIndex = lastIndexOf('.', filename);
    if (lastIndex < 0) {
      return null;
    }

    CharSequence ext = filename.subSequence(lastIndex, filename.length());

    for (FileFormat format : FileFormat.values()) {
      if (Comparators.charSequences().compare(format.ext, ext) == 0) {
        return format;
      }
    }

    return null;
  }

  private static int lastIndexOf(char ch, CharSequence seq) {
    for (int i = seq.length() - 1; i >= 0; i -= 1) {
      if (seq.charAt(i) == ch) {
        return i;
      }
    }
    return -1;
  }
}

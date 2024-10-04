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
package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

public class ContentFileUtil {
  private ContentFileUtil() {}

  /**
   * Copies the {@link ContentFile} with the specific stat settings.
   *
   * @param file a generic data file to copy.
   * @param withStats whether to keep any stats
   * @param requestedColumnIds column ids for which to keep stats. If <code>null</code> then every
   *     column stat is kept.
   * @return The copied file
   */
  public static <F extends ContentFile<K>, K> K copy(
      F file, boolean withStats, Set<Integer> requestedColumnIds) {
    if (withStats) {
      return requestedColumnIds != null ? file.copyWithStats(requestedColumnIds) : file.copy();
    } else {
      return file.copyWithoutStats();
    }
  }

  public static CharSequence referencedDataFile(DeleteFile deleteFile) {
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      return null;
    }

    int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    Type pathType = MetadataColumns.DELETE_FILE_PATH.type();

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(pathId) : null;
    if (lowerPathBound == null) {
      return null;
    }

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(pathId) : null;
    if (upperPathBound == null) {
      return null;
    }

    if (lowerPathBound.equals(upperPathBound)) {
      return Conversions.fromByteBuffer(pathType, lowerPathBound);
    } else {
      return null;
    }
  }

  public static String referencedDataFileLocation(DeleteFile deleteFile) {
    CharSequence location = referencedDataFile(deleteFile);
    return location != null ? location.toString() : null;
  }
}

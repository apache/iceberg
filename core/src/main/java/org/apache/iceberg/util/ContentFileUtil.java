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
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

public class ContentFileUtil {
  private ContentFileUtil() {}

  private static final int PATH_ID = MetadataColumns.DELETE_FILE_PATH.fieldId();
  private static final Type PATH_TYPE = MetadataColumns.DELETE_FILE_PATH.type();

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

    if (deleteFile.referencedDataFile() != null) {
      return deleteFile.referencedDataFile();
    }

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(PATH_ID) : null;
    if (lowerPathBound == null) {
      return null;
    }

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(PATH_ID) : null;
    if (upperPathBound == null) {
      return null;
    }

    if (lowerPathBound.equals(upperPathBound)) {
      return Conversions.fromByteBuffer(PATH_TYPE, lowerPathBound);
    } else {
      return null;
    }
  }

  /**
   * Replace file_path reference for a delete file manifest entry, if file_path field's lower_bound
   * and upper_bound metrics are equal. Else clear file_path lower and upper bounds.
   *
   * @param deleteFile delete file whose entry will be replaced
   * @param sourcePrefix source prefix which will be replaced
   * @param targetPrefix target prefix which will replace it
   * @return metrics for the new delete file entry
   */
  public static Metrics replacePathBounds(
      DeleteFile deleteFile, String sourcePrefix, String targetPrefix) {
    Preconditions.checkArgument(
        deleteFile.content() == FileContent.POSITION_DELETES,
        "Only position delete files supported");

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(PATH_ID) : null;
    if (lowerPathBound == null) {
      return metricsWithoutPathBounds(deleteFile);
    }

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(PATH_ID) : null;
    if (upperPathBound == null) {
      return metricsWithoutPathBounds(deleteFile);
    }

    if (lowerPathBound.equals(upperPathBound)) {
      CharBuffer path = Conversions.fromByteBuffer(PATH_TYPE, lowerPathBound);
      CharBuffer newPath =
          CharBuffer.wrap(
              RewriteTablePathUtil.newPath(path.toString(), sourcePrefix, targetPrefix));
      ByteBuffer newBytes = Conversions.toByteBuffer(PATH_TYPE, newPath);
      return metricsWithPathBounds(deleteFile, newBytes);
    } else {
      // The file_path's lower_bound and upper_bound are only used for filtering data files when
      // both values match
      // (file-scoped position delete). Hence do not rewrite but set null if they do not match.
      return metricsWithoutPathBounds(deleteFile);
    }
  }

  public static String referencedDataFileLocation(DeleteFile deleteFile) {
    CharSequence location = referencedDataFile(deleteFile);
    return location != null ? location.toString() : null;
  }

  public static boolean isFileScoped(DeleteFile deleteFile) {
    return referencedDataFile(deleteFile) != null;
  }

  public static boolean isDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN;
  }

  public static boolean containsSingleDV(Iterable<DeleteFile> deleteFiles) {
    return Iterables.size(deleteFiles) == 1 && Iterables.all(deleteFiles, ContentFileUtil::isDV);
  }

  public static String dvDesc(DeleteFile deleteFile) {
    return String.format(
        "DV{location=%s, offset=%s, length=%s, referencedDataFile=%s}",
        deleteFile.location(),
        deleteFile.contentOffset(),
        deleteFile.contentSizeInBytes(),
        deleteFile.referencedDataFile());
  }

  private static Metrics metricsWithoutPathBounds(DeleteFile file) {
    Map<Integer, ByteBuffer> lowerBounds =
        file.lowerBounds() == null ? null : Maps.newHashMap(file.lowerBounds());
    Map<Integer, ByteBuffer> upperBounds =
        file.upperBounds() == null ? null : Maps.newHashMap(file.upperBounds());
    if (lowerBounds != null) {
      lowerBounds.remove(PATH_ID);
    }
    if (upperBounds != null) {
      upperBounds.remove(PATH_ID);
    }

    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        lowerBounds == null ? null : Collections.unmodifiableMap(lowerBounds),
        upperBounds == null ? null : Collections.unmodifiableMap(upperBounds));
  }

  private static Metrics metricsWithPathBounds(DeleteFile file, ByteBuffer bound) {
    Map<Integer, ByteBuffer> lowerBounds =
        file.lowerBounds() == null ? null : Maps.newHashMap(file.lowerBounds());
    Map<Integer, ByteBuffer> upperBounds =
        file.upperBounds() == null ? null : Maps.newHashMap(file.upperBounds());
    if (lowerBounds != null) {
      lowerBounds.put(PATH_ID, bound);
    }
    if (upperBounds != null) {
      upperBounds.put(PATH_ID, bound);
    }

    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        lowerBounds == null ? null : Collections.unmodifiableMap(lowerBounds),
        upperBounds == null ? null : Collections.unmodifiableMap(upperBounds));
  }
}

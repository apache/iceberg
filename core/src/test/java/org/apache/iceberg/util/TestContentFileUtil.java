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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.Test;

public class TestContentFileUtil {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final int PATH_ID = MetadataColumns.DELETE_FILE_PATH.fieldId();
  private static final Type PATH_TYPE = MetadataColumns.DELETE_FILE_PATH.type();
  private static final int POS_ID = MetadataColumns.DELETE_FILE_POS.fieldId();

  @Test
  public void referencedDataFileReturnsNullForEqualityDeletes() {
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/equality-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThat(ContentFileUtil.referencedDataFile(equalityDelete)).isNull();
  }

  @Test
  public void referencedDataFileReturnsExplicitReference() {
    DeleteFile positionDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .withReferencedDataFile("/path/to/data.parquet")
            .build();

    assertThat(ContentFileUtil.referencedDataFile(positionDelete))
        .isEqualTo("/path/to/data.parquet");
  }

  @Test
  public void referencedDataFileReturnsPathFromEqualBounds() {
    String dataFile = "/path/to/data.parquet";
    DeleteFile positionDelete = positionDeleteWithPathBounds(dataFile, dataFile);

    assertThat(ContentFileUtil.referencedDataFile(positionDelete)).hasToString(dataFile);
  }

  @Test
  public void referencedDataFileReturnsNullWhenBoundsDiffer() {
    DeleteFile positionDelete =
        positionDeleteWithPathBounds("/path/to/data-1.parquet", "/path/to/data-2.parquet");

    assertThat(ContentFileUtil.referencedDataFile(positionDelete)).isNull();
  }

  @Test
  public void referencedDataFileReturnsNullWhenBoundsMissing() {
    DeleteFile positionDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThat(ContentFileUtil.referencedDataFile(positionDelete)).isNull();
  }

  @Test
  public void referencedDataFileReturnsNullWhenUpperBoundMissing() {
    // lower bound carries the path, but the upper bound for the path column is absent
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    lowerBounds.put(PATH_ID, pathBound("/path/to/data.parquet"));
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    upperBounds.put(POS_ID, posBound(10L));
    DeleteFile positionDelete = positionDeleteWithBounds(lowerBounds, upperBounds);

    assertThat(ContentFileUtil.referencedDataFile(positionDelete)).isNull();
  }

  @Test
  public void referencedDataFileLocation() {
    DeleteFile withRef =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .withReferencedDataFile("/path/to/data.parquet")
            .build();
    DeleteFile withoutRef =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThat(ContentFileUtil.referencedDataFileLocation(withRef))
        .isEqualTo("/path/to/data.parquet");
    assertThat(ContentFileUtil.referencedDataFileLocation(withoutRef)).isNull();
  }

  @Test
  public void isFileScoped() {
    DeleteFile fileScoped =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .withReferencedDataFile("/path/to/data.parquet")
            .build();
    DeleteFile notFileScoped =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThat(ContentFileUtil.isFileScoped(fileScoped)).isTrue();
    assertThat(ContentFileUtil.isFileScoped(notFileScoped)).isFalse();
  }

  @Test
  public void isDV() {
    assertThat(ContentFileUtil.isDV(dv())).isTrue();

    DeleteFile parquetDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();
    assertThat(ContentFileUtil.isDV(parquetDelete)).isFalse();
  }

  @Test
  public void containsSingleDV() {
    DeleteFile dv = dv();
    DeleteFile parquetDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThat(ContentFileUtil.containsSingleDV(ImmutableList.of(dv))).isTrue();
    assertThat(ContentFileUtil.containsSingleDV(ImmutableList.of())).isFalse();
    assertThat(ContentFileUtil.containsSingleDV(ImmutableList.of(dv(), dv()))).isFalse();
    assertThat(ContentFileUtil.containsSingleDV(ImmutableList.of(parquetDelete))).isFalse();
  }

  @Test
  public void dvDesc() {
    DeleteFile dv = dv();

    assertThat(ContentFileUtil.dvDesc(dv))
        .contains("location=" + dv.location())
        .contains("offset=" + dv.contentOffset())
        .contains("length=" + dv.contentSizeInBytes())
        .contains("referencedDataFile=" + dv.referencedDataFile());
  }

  @Test
  public void replacePathBoundsRejectsNonPositionDeletes() {
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/equality-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    assertThatThrownBy(() -> ContentFileUtil.replacePathBounds(equalityDelete, "/old", "/new"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only position delete files supported");
  }

  @Test
  public void replacePathBoundsRewritesEqualBounds() {
    String sourcePrefix = "/old/table";
    String targetPrefix = "/new/table";
    String dataFile = sourcePrefix + "/data/file.parquet";
    DeleteFile positionDelete = positionDeleteWithPathBounds(dataFile, dataFile);

    Metrics metrics = ContentFileUtil.replacePathBounds(positionDelete, sourcePrefix, targetPrefix);

    String expected = RewriteTablePathUtil.newPath(dataFile, sourcePrefix, targetPrefix);
    assertThat(expected).isNotEqualTo(dataFile);
    assertThat(decodePathBound(metrics.lowerBounds())).isEqualTo(expected);
    assertThat(decodePathBound(metrics.upperBounds())).isEqualTo(expected);
    // non-path bounds must be carried over untouched
    assertThat(metrics.lowerBounds()).containsKey(POS_ID);
    assertThat(metrics.upperBounds()).containsKey(POS_ID);
  }

  @Test
  public void replacePathBoundsClearsWhenUpperBoundMissing() {
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    lowerBounds.put(PATH_ID, pathBound("/old/table/data.parquet"));
    lowerBounds.put(POS_ID, posBound(0L));
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    upperBounds.put(POS_ID, posBound(10L));
    DeleteFile positionDelete = positionDeleteWithBounds(lowerBounds, upperBounds);

    Metrics metrics = ContentFileUtil.replacePathBounds(positionDelete, "/old", "/new");

    // upper path bound missing -> path bounds dropped, position bound preserved
    assertThat(metrics.lowerBounds()).doesNotContainKey(PATH_ID).containsKey(POS_ID);
    assertThat(metrics.upperBounds()).doesNotContainKey(PATH_ID).containsKey(POS_ID);
  }

  @Test
  public void replacePathBoundsClearsUnequalBounds() {
    DeleteFile positionDelete =
        positionDeleteWithPathBounds("/old/table/data-1.parquet", "/old/table/data-2.parquet");

    Metrics metrics = ContentFileUtil.replacePathBounds(positionDelete, "/old", "/new");

    // path bounds are dropped, but the position column bound is preserved
    assertThat(metrics.lowerBounds()).doesNotContainKey(PATH_ID).containsKey(POS_ID);
    assertThat(metrics.upperBounds()).doesNotContainKey(PATH_ID).containsKey(POS_ID);
  }

  @Test
  public void replacePathBoundsClearsMissingBounds() {
    DeleteFile positionDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(5)
            .build();

    Metrics metrics = ContentFileUtil.replacePathBounds(positionDelete, "/old", "/new");

    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  public void copyWithoutStatsDropsBounds() {
    DeleteFile positionDelete =
        positionDeleteWithPathBounds("/path/to/data.parquet", "/path/to/data.parquet");

    DeleteFile copy = ContentFileUtil.copy(positionDelete, false, null);

    assertThat(copy.lowerBounds()).isNull();
    assertThat(copy.upperBounds()).isNull();
  }

  @Test
  public void copyWithAllStatsKeepsBounds() {
    DeleteFile positionDelete =
        positionDeleteWithPathBounds("/path/to/data.parquet", "/path/to/data.parquet");

    DeleteFile copy = ContentFileUtil.copy(positionDelete, true, null);

    assertThat(copy.lowerBounds()).containsKey(PATH_ID);
    assertThat(copy.upperBounds()).containsKey(PATH_ID);
  }

  @Test
  public void copyWithRequestedStatsKeepsOnlyRequestedColumns() {
    DeleteFile positionDelete =
        positionDeleteWithPathBounds("/path/to/data.parquet", "/path/to/data.parquet");

    DeleteFile kept = ContentFileUtil.copy(positionDelete, true, ImmutableSet.of(PATH_ID));
    assertThat(kept.lowerBounds()).containsKey(PATH_ID);

    DeleteFile dropped = ContentFileUtil.copy(positionDelete, true, ImmutableSet.of(POS_ID));
    assertThat(dropped.lowerBounds()).doesNotContainKey(PATH_ID);
  }

  private DeleteFile dv() {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath("/path/to/delete.puffin")
        .withFileSizeInBytes(1024)
        .withRecordCount(5)
        .withReferencedDataFile("/path/to/data.parquet")
        .withContentOffset(4)
        .withContentSizeInBytes(40)
        .build();
  }

  private DeleteFile positionDeleteWithPathBounds(String lowerPath, String upperPath) {
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    lowerBounds.put(PATH_ID, pathBound(lowerPath));
    upperBounds.put(PATH_ID, pathBound(upperPath));
    // include a position-column bound so bound-clearing behavior can be verified
    lowerBounds.put(POS_ID, posBound(0L));
    upperBounds.put(POS_ID, posBound(10L));
    return positionDeleteWithBounds(lowerBounds, upperBounds);
  }

  private DeleteFile positionDeleteWithBounds(
      Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds) {
    Metrics metrics = new Metrics(5L, null, null, null, null, lowerBounds, upperBounds);
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath("/path/to/position-delete.parquet")
        .withFileSizeInBytes(1024)
        .withMetrics(metrics)
        .build();
  }

  private static ByteBuffer pathBound(String path) {
    return Conversions.toByteBuffer(PATH_TYPE, path);
  }

  private static ByteBuffer posBound(long pos) {
    return Conversions.toByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), pos);
  }

  private String decodePathBound(Map<Integer, ByteBuffer> bounds) {
    return Conversions.fromByteBuffer(PATH_TYPE, bounds.get(PATH_ID)).toString();
  }
}

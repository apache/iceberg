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
package org.apache.iceberg.deletes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BitmapPositionDeleteIndex implements PositionDeleteIndex {
  private static final int LENGTH_SIZE_BYTES = 4;
  private static final int MAGIC_NUMBER_SIZE_BYTES = 4;
  private static final int CRC_SIZE_BYTES = 4;
  private static final int BITMAP_DATA_OFFSET = 4;
  private static final int MAGIC_NUMBER = 1681511377;

  private final RoaringPositionBitmap bitmap;
  private final List<DeleteFile> deleteFiles;

  BitmapPositionDeleteIndex() {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = Lists.newArrayList();
  }

  BitmapPositionDeleteIndex(Collection<DeleteFile> deleteFiles) {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = Lists.newArrayList(deleteFiles);
  }

  BitmapPositionDeleteIndex(DeleteFile deleteFile) {
    this.bitmap = new RoaringPositionBitmap();
    this.deleteFiles = deleteFile != null ? Lists.newArrayList(deleteFile) : Lists.newArrayList();
  }

  BitmapPositionDeleteIndex(RoaringPositionBitmap bitmap, DeleteFile deleteFile) {
    this.bitmap = bitmap;
    this.deleteFiles = deleteFile != null ? Lists.newArrayList(deleteFile) : Lists.newArrayList();
  }

  void merge(BitmapPositionDeleteIndex that) {
    bitmap.setAll(that.bitmap);
    deleteFiles.addAll(that.deleteFiles);
  }

  @Override
  public void delete(long position) {
    bitmap.set(position);
  }

  @Override
  public void delete(long posStart, long posEnd) {
    bitmap.setRange(posStart, posEnd);
  }

  @Override
  public void merge(PositionDeleteIndex that) {
    if (that instanceof BitmapPositionDeleteIndex) {
      merge((BitmapPositionDeleteIndex) that);
    } else {
      that.forEach(this::delete);
      deleteFiles.addAll(that.deleteFiles());
    }
  }

  @Override
  public boolean isDeleted(long position) {
    return bitmap.contains(position);
  }

  @Override
  public boolean isEmpty() {
    return bitmap.isEmpty();
  }

  @Override
  public void forEach(LongConsumer consumer) {
    bitmap.forEach(consumer);
  }

  @Override
  public Collection<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  @Override
  public long cardinality() {
    return bitmap.cardinality();
  }

  /**
   * Serializes the index using the following format:
   *
   * <ul>
   *   <li>The length of the magic bytes and bitmap stored as 4 bytes (big-endian).
   *   <li>A 4-byte {@link #MAGIC_NUMBER} (little-endian).
   *   <li>The bitmap serialized using the portable Roaring spec (little-endian).
   *   <li>A CRC-32 checksum of the magic bytes and bitmap as 4-bytes (big-endian).
   * </ul>
   *
   * Note that the length and the checksum are computed for the bitmap data, which includes the
   * magic bytes and bitmap for compatibility with Delta.
   */
  @Override
  public ByteBuffer serialize() {
    bitmap.runLengthEncode(); // run-length encode the bitmap before serializing
    int bitmapDataLength = computeBitmapDataLength(bitmap); // magic bytes + bitmap
    byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(bitmapDataLength);
    serializeBitmapData(bytes, bitmapDataLength, bitmap);
    int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
    int crc = computeChecksum(bytes, bitmapDataLength);
    buffer.putInt(crcOffset, crc);
    buffer.rewind();
    return buffer;
  }

  /**
   * Deserializes the index from bytes, assuming the format described in {@link #serialize()}.
   *
   * @param bytes an array containing the serialized index
   * @param deleteFile the DV file
   * @return the deserialized index
   */
  public static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int bitmapDataLength = readBitmapDataLength(buffer, deleteFile);
    RoaringPositionBitmap bitmap = deserializeBitmap(bytes, bitmapDataLength, deleteFile);
    int crc = computeChecksum(bytes, bitmapDataLength);
    int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
    int expectedCrc = buffer.getInt(crcOffset);
    Preconditions.checkArgument(crc == expectedCrc, "Invalid CRC");
    return new BitmapPositionDeleteIndex(bitmap, deleteFile);
  }

  // computes and validates the length of the bitmap data (magic bytes + bitmap)
  private static int computeBitmapDataLength(RoaringPositionBitmap bitmap) {
    long length = MAGIC_NUMBER_SIZE_BYTES + bitmap.serializedSizeInBytes();
    long bufferSize = LENGTH_SIZE_BYTES + length + CRC_SIZE_BYTES;
    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Can't serialize index > 2GB");
    return (int) length;
  }

  // serializes the bitmap data (magic bytes + bitmap) using the little-endian byte order
  private static void serializeBitmapData(
      byte[] bytes, int bitmapDataLength, RoaringPositionBitmap bitmap) {
    ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
    bitmapData.putInt(MAGIC_NUMBER);
    bitmap.serialize(bitmapData);
  }

  // points to the bitmap data in the blob
  private static ByteBuffer pointToBitmapData(byte[] bytes, int bitmapDataLength) {
    ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
    bitmapData.order(ByteOrder.LITTLE_ENDIAN);
    return bitmapData;
  }

  // checks the blob size is equal to the bitmap data length + extra bytes for length and CRC
  private static int readBitmapDataLength(ByteBuffer buffer, DeleteFile deleteFile) {
    int length = buffer.getInt();
    long expectedLength = deleteFile.contentSizeInBytes() - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
    Preconditions.checkArgument(
        length == expectedLength,
        "Invalid bitmap data length: %s, expected %s",
        length,
        expectedLength);
    return length;
  }

  // validates magic bytes and deserializes the bitmap
  private static RoaringPositionBitmap deserializeBitmap(
      byte[] bytes, int bitmapDataLength, DeleteFile deleteFile) {
    ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
    int magicNumber = bitmapData.getInt();
    Preconditions.checkArgument(
        magicNumber == MAGIC_NUMBER,
        "Invalid magic number: %s, expected %s",
        magicNumber,
        MAGIC_NUMBER);
    RoaringPositionBitmap bitmap = RoaringPositionBitmap.deserialize(bitmapData);
    long cardinality = bitmap.cardinality();
    long expectedCardinality = deleteFile.recordCount();
    Preconditions.checkArgument(
        cardinality == expectedCardinality,
        "Invalid cardinality: %s, expected %s",
        cardinality,
        expectedCardinality);
    return bitmap;
  }

  // generates a 32-bit unsigned checksum for the magic bytes and serialized bitmap
  private static int computeChecksum(byte[] bytes, int bitmapDataLength) {
    CRC32 crc = new CRC32();
    crc.update(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
    return (int) crc.getValue();
  }
}

package com.netflix.iceberg.encryption;

import java.io.Serializable;
import java.nio.ByteBuffer;

class GenericEncryptionKeyMetadata implements EncryptionKeyMetadata, Serializable {

  private final byte[] keyMetadata;

  private GenericEncryptionKeyMetadata(byte[] keyMetadata) {
    this.keyMetadata = keyMetadata;
  }

  static GenericEncryptionKeyMetadata of(byte[] keyMetadata) {
    return new GenericEncryptionKeyMetadata(keyMetadata);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return ByteBuffer.wrap(keyMetadata).asReadOnlyBuffer();
  }

  @Override
  public EncryptionKeyMetadata copy() {
    byte[] keyMetadataBytesCopy = new byte[keyMetadata.length];
    System.arraycopy(keyMetadata, 0, keyMetadataBytesCopy, 0, keyMetadata.length);
    return GenericEncryptionKeyMetadata.of(keyMetadataBytesCopy);
  }
}

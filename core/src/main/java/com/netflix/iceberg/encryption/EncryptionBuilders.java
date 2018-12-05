package com.netflix.iceberg.encryption;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import org.apache.avro.Schema;

import java.nio.ByteBuffer;

public class EncryptionBuilders {
  private EncryptionBuilders() {}

  public static FileEncryptionMetadataBuilder newFileEncryptionMetadataBuilder() {
    return new FileEncryptionMetadataBuilder();
  }

  public static KeyDescriptionBuilder newKeyDescriptionBuilder() {
    return new KeyDescriptionBuilder();
  }

  public static class FileEncryptionMetadataBuilder {
    private final Schema schema = AvroSchemaUtil.convert(EncryptionTypes.ENCRYPTION_METADATA_TYPE);
    private EncryptionTypes.KeyDescription keyDescription;
    private ByteBuffer iv = null;
    private ByteBuffer encryptedKey = null;
    private String keyAlgorithm = null;

    private FileEncryptionMetadataBuilder() {}

    public FileEncryptionMetadataBuilder withKeyDescription(EncryptionTypes.KeyDescription keyDescription) {
      this.keyDescription = keyDescription;
      return this;
    }

    public FileEncryptionMetadataBuilder withIv(ByteBuffer iv) {
      this.iv = iv;
      return this;
    }

    public FileEncryptionMetadataBuilder withEncryptedKey(ByteBuffer encryptedKey) {
      this.encryptedKey = encryptedKey;
      return this;
    }

    public FileEncryptionMetadataBuilder withKeyAlgorithm(String keyAlgorithm) {
      this.keyAlgorithm = keyAlgorithm;
      return this;
    }

    public EncryptionTypes.FileEncryptionMetadata build() {
      Preconditions.checkNotNull(keyDescription, "Key description is required.");
      Preconditions.checkNotNull(iv, "Iv is required.");
      Preconditions.checkNotNull(encryptedKey, "Encrypted key is required.");
      Preconditions.checkNotNull(keyAlgorithm, "Key algorithm is required.");
      return new GenericFileEncryptionMetadata(
          schema, keyDescription, iv, encryptedKey, keyAlgorithm);
    }
  }

  public static class KeyDescriptionBuilder {
    private final Schema schema = AvroSchemaUtil.convert(EncryptionTypes.KEY_DESCRIPTION_TYPE);
    private String keyName;
    // Boxed so we can check for nullability
    private Integer keyVersion;

    private KeyDescriptionBuilder() {}

    public KeyDescriptionBuilder withKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public KeyDescriptionBuilder withKeyVersion(int keyVersion) {
      this.keyVersion = keyVersion;
      return this;
    }

    public EncryptionTypes.KeyDescription build() {
      Preconditions.checkNotNull(keyName, "Key name is required.");
      Preconditions.checkNotNull(keyVersion, "Key version is required.");
      return new GenericKeyDescription(schema, keyName, keyVersion);
    }
  }
}

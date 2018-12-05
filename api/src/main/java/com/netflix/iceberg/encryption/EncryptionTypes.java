package com.netflix.iceberg.encryption;

import com.netflix.iceberg.types.Types;

import java.nio.ByteBuffer;

public class EncryptionTypes {

  private EncryptionTypes() {}

  public static final Types.StructType KEY_DESCRIPTION_TYPE = Types.StructType.of(
      Types.NestedField.required(10000, "key_name", Types.StringType.get()),
      Types.NestedField.required(10001, "key_version", Types.IntegerType.get()));

  public static final Types.StructType ENCRYPTION_METADATA_TYPE = Types.StructType.of(
      Types.NestedField.required(9000, "key_description", KEY_DESCRIPTION_TYPE),
      Types.NestedField.required(9001, "iv", Types.BinaryType.get()),
      Types.NestedField.required(9002, "encrypted_key", Types.BinaryType.get()),
      Types.NestedField.required(9003, "key_algorithm", Types.StringType.get()));

  public interface KeyDescription {

    String keyName();

    int keyVersion();

    KeyDescription copy();
  }

  public interface FileKey {
    KeyDescription keyDescription();

    ByteBuffer decryptedKey();

    ByteBuffer encryptedKey();
  }

  public interface FileEncryptionMetadata {
    KeyDescription keyDescription();

    String keyAlgorithm();

    ByteBuffer iv();

    ByteBuffer encryptedKey();

    FileEncryptionMetadata copy();
  }

}

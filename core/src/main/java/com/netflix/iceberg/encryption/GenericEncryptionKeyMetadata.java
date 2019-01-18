package com.netflix.iceberg.encryption;

import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.ByteBuffers;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import sun.security.krb5.EncryptionKey;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class GenericEncryptionKeyMetadata implements
    EncryptionKeyMetadata,
    StructLike,
    IndexedRecord,
    SpecificData.SchemaConstructable,
    Serializable {

  private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(
      EncryptionKeyMetadata.schema(), "encryption_metadata");

  // Everything is not final for serialization.
  private transient Schema avroSchema;
  private int[] fromProjectionPos;

  private byte[] keyMetadata;
  private String cipherAlgorithm;
  private String keyAlgorithm;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericEncryptionKeyMetadata(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;

    List<Types.NestedField> fields = AvroSchemaUtil.convert(avroSchema)
        .asNestedType()
        .asStructType()
        .fields();
    List<Types.NestedField> allFields = EncryptionKeyMetadata.schema().asStruct().fields();

    this.fromProjectionPos = new int[fields.size()];
    for (int i = 0; i < fromProjectionPos.length; i += 1) {
      boolean found = false;
      for (int j = 0; j < allFields.size(); j += 1) {
        if (fields.get(i).fieldId() == allFields.get(j).fieldId()) {
          found = true;
          fromProjectionPos[i] = j;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
      }
    }
  }

  GenericEncryptionKeyMetadata(
      byte[] keyMetadata,
      String cipherAlgorithm,
      String keyAlgorithm) {
    this.keyMetadata = keyMetadata;
    this.cipherAlgorithm = cipherAlgorithm;
    this.keyAlgorithm = keyAlgorithm;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return ByteBuffer.wrap(keyMetadata).asReadOnlyBuffer();
  }

  @Override
  public String cipherAlgorithm() {
    return cipherAlgorithm;
  }

  @Override
  public String keyAlgorithm() {
    return keyAlgorithm;
  }

  @Override
  public EncryptionKeyMetadata copy() {
    byte[] keyMetadataBytesCopy = new byte[keyMetadata.length];
    System.arraycopy(keyMetadata, 0, keyMetadataBytesCopy, 0, keyMetadata.length);
    return new GenericEncryptionKeyMetadata(keyMetadataBytesCopy, cipherAlgorithm, keyAlgorithm);
  }

  @Override
  public int size() {
    return EncryptionKeyMetadata.schema().columns().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public void put(int i, Object v) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) v);
        return;
      case 1:
        this.cipherAlgorithm = v.toString();
        return;
      case 2:
        this.keyAlgorithm = v.toString();
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        return keyMetadata == null ? null : keyMetadata();
      case 1:
        return cipherAlgorithm;
      case 2:
        return keyAlgorithm;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}

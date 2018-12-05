package com.netflix.iceberg.encryption;

import com.netflix.iceberg.util.ByteBuffers;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class GenericFileEncryptionMetadata implements
    EncryptionTypes.FileEncryptionMetadata, IndexedRecord, SpecificData.SchemaConstructable, Serializable {

  private final org.apache.avro.Schema schema;

  private EncryptionTypes.KeyDescription keyDescription;
  private transient ByteBuffer iv = null;
  private transient ByteBuffer encryptedKey = null;
  private String keyAlgorithm = null;

  public GenericFileEncryptionMetadata(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  GenericFileEncryptionMetadata(
      org.apache.avro.Schema schema,
      EncryptionTypes.KeyDescription keyDescription,
      ByteBuffer iv,
      ByteBuffer encryptedKey,
      String keyAlgorithm) {
    this.schema = schema;
    this.keyDescription = keyDescription;
    this.iv = iv;
    this.encryptedKey = encryptedKey;
    this.keyAlgorithm  = keyAlgorithm;
  }

  GenericFileEncryptionMetadata(GenericFileEncryptionMetadata toCopy) {
    this.schema = toCopy.getSchema();
    this.keyDescription = toCopy.keyDescription().copy();
    this.iv = ByteBuffer.wrap(ByteBuffers.copy(toCopy.iv()));
    this.encryptedKey = ByteBuffer.wrap(ByteBuffers.copy(toCopy.encryptedKey()));
    this.keyAlgorithm = toCopy.keyAlgorithm();
  }

  @Override
  public EncryptionTypes.KeyDescription keyDescription() {
    return keyDescription;
  }

  @Override
  public String keyAlgorithm() {
    return keyAlgorithm;
  }

  @Override
  public ByteBuffer iv() {
    return iv;
  }

  @Override
  public ByteBuffer encryptedKey() {
    return encryptedKey;
  }

  @Override
  public EncryptionTypes.FileEncryptionMetadata copy() {
    return new GenericFileEncryptionMetadata(this);
  }

  @Override
  public void put(int pos, Object v) {
    switch(pos) {
      case 0:
        this.keyDescription = (EncryptionTypes.KeyDescription) v;
        break;
      case 1:
        this.iv = ByteBuffer.wrap(ByteBuffers.copy((ByteBuffer) v));
        break;
      case 2:
        this.encryptedKey = ByteBuffer.wrap(ByteBuffers.copy((ByteBuffer) v));
        break;
      case 3:
        this.keyAlgorithm = v.toString();
        break;
      default:
        throw new IndexOutOfBoundsException(String.format("Index %d must be 0, 1, 2, or 3.", pos));
    }
  }

  @Override
  public Object get(int pos) {
    switch(pos) {
      case 0:
        return this.keyDescription;
      case 1:
        return this.iv;
      case 2:
        return this.encryptedKey;
      case 3:
        return this.keyAlgorithm;
      default:
        throw new IndexOutOfBoundsException(String.format("Index %d must be 0, 1, 2, or 3.", pos));
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  private void writeObject(ObjectOutputStream outputStream) throws ClassNotFoundException, IOException {
    outputStream.defaultWriteObject();
    byte[] ivBytes = ByteBuffers.copy(iv);
    outputStream.writeInt(ivBytes.length);
    outputStream.write(ivBytes);
    byte[] encryptedKeyBytes = ByteBuffers.copy(encryptedKey);
    outputStream.writeInt(encryptedKeyBytes.length);
    outputStream.write(encryptedKeyBytes);
  }

  private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
    inputStream.defaultReadObject();
    byte[] ivBytes = new byte[inputStream.readInt()];
    inputStream.read(ivBytes, 0, ivBytes.length);
    byte[] encryptedKeyBytes = new byte[inputStream.readInt()];
    inputStream.read(encryptedKeyBytes, 0, encryptedKeyBytes.length);
    this.iv = ByteBuffer.wrap(ivBytes).asReadOnlyBuffer();
    this.encryptedKey = ByteBuffer.wrap(encryptedKeyBytes).asReadOnlyBuffer();
  }
}

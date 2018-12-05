package com.netflix.iceberg.encryption;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.Serializable;

public class GenericKeyDescription implements
    EncryptionTypes.KeyDescription,
    IndexedRecord,
    SpecificData.SchemaConstructable,
    Serializable {

  private final Schema schema;
  private String keyName;
  private int keyVersion;

  public GenericKeyDescription(Schema schema) {
    this.schema = schema;
  }

  GenericKeyDescription(Schema schema, String keyName, int keyVersion) {
    this.schema = schema;
    this.keyName = keyName;
    this.keyVersion = keyVersion;
  }

  GenericKeyDescription(GenericKeyDescription other) {
    this.schema = other.getSchema();
    this.keyName = other.keyName();
    this.keyVersion = other.keyVersion();
  }

  @Override
  public String keyName() {
    return keyName;
  }

  @Override
  public int keyVersion() {
    return keyVersion;
  }

  @Override
  public GenericKeyDescription copy() {
    return new GenericKeyDescription(this);
  }

  @Override
  public void put(int pos, Object v) {
    switch(pos) {
      case 0:
        this.keyName = v.toString();
        break;
      case 1:
        this.keyVersion = (Integer) v;
        break;
      default: throw new IndexOutOfBoundsException(String.format("Index %d must be 0 or 1.", pos));
    }
  }

  @Override
  public Object get(int pos) {
    switch(pos) {
      case 0: return this.keyName;
      case 1: return this.keyVersion;
      default: throw new IndexOutOfBoundsException(String.format("Index %d must be 0 or 1.", pos));
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}

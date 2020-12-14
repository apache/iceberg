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

package org.apache.iceberg.flink;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;

/**
 * This util class holds the serializable parts of {@link org.apache.iceberg.Table}
 */
public class TableInfo implements Serializable {

  private final String location;
  private final Schema schema;
  private final PartitionSpec spec;
  private final Map<String, String> properties;
  private final FileIO fileIO;
  private final EncryptionManager encryptionManager;

  public TableInfo(String location, Schema schema, PartitionSpec spec,
                   Map<String, String> properties, FileIO fileIO,
                   EncryptionManager encryptionManager) {
    this.location = location;
    this.schema = schema;
    this.spec = spec;
    this.properties = properties;
    this.fileIO = fileIO;
    this.encryptionManager = encryptionManager;
  }

  public static TableInfo fromTable(Table table) {
    return TableInfo.builder()
        .location(table.location())
        .schema(table.schema())
        .spec(table.spec())
        .properties(table.properties())
        .fileIO(table.io())
        .encryptionManager(table.encryption())
        .build();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private String location;
    private Schema schema;
    private PartitionSpec spec;
    private Map<String, String> properties;
    private FileIO fileIO;
    private EncryptionManager encryptionManager;

    public Builder location(String newLocation) {
      this.location = newLocation;
      return this;
    }

    public Builder schema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    public Builder spec(PartitionSpec newSpec) {
      this.spec = newSpec;
      return this;
    }

    public Builder properties(Map<String, String> newProperties) {
      this.properties = newProperties;
      return this;
    }

    public Builder fileIO(FileIO newFileIO) {
      this.fileIO = newFileIO;
      return this;
    }

    public Builder encryptionManager(EncryptionManager newEncryptionManager) {
      this.encryptionManager = newEncryptionManager;
      return this;
    }

    public TableInfo build() {
      return new TableInfo(location, schema, spec, properties, fileIO, encryptionManager);
    }
  }

  public String location() {
    return location;
  }

  public Schema schema() {
    return schema;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public FileIO fileIO() {
    return fileIO;
  }

  public EncryptionManager encryptionManager() {
    return encryptionManager;
  }
}

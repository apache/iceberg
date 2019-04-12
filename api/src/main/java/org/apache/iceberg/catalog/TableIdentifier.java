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
package org.apache.iceberg.catalog;

/**
 * Identifies a table in iceberg catalog
 */
public class TableIdentifier {
  private final SchemaIdentifier schemaIdentifier;
  private final String tableName;

  public TableIdentifier(SchemaIdentifier schemaIdentifier, String tableName) {
    if(schemaIdentifier == null) {
      throw new IllegalArgumentException("schemaIdentifier can not be null");
    }

    if(tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("tableName can not be null");
    }

    this.schemaIdentifier = schemaIdentifier;
    this.tableName = tableName;
  }

  public SchemaIdentifier schemaNameIdentifier() {
    return schemaIdentifier;
  }

  public String tableName() {
    return tableName;
  }
}

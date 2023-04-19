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
package org.apache.iceberg.snowflake;

import java.io.Closeable;
import java.util.List;

/**
 * This interface abstracts out the underlying communication protocols for contacting Snowflake to
 * obtain the various resource representations defined under "entities". Classes using this
 * interface should minimize assumptions about whether an underlying client uses e.g. REST, JDBC or
 * other underlying libraries/protocols.
 */
interface SnowflakeClient extends Closeable {

  /** Returns true if the database exists, false otherwise. */
  boolean databaseExists(SnowflakeIdentifier database);

  /** Returns true if the schema and its parent database exists, false otherwise. */
  boolean schemaExists(SnowflakeIdentifier schema);

  /** Lists all Snowflake databases within the currently configured account. */
  List<SnowflakeIdentifier> listDatabases();

  /**
   * Lists all Snowflake schemas within a given scope. Returned SnowflakeIdentifiers must have
   * type() == SnowflakeIdentifier.Type.SCHEMA.
   *
   * @param scope The scope in which to list, which may be ROOT or a single DATABASE.
   */
  List<SnowflakeIdentifier> listSchemas(SnowflakeIdentifier scope);

  /**
   * Lists all Snowflake Iceberg tables within a given scope. Returned SnowflakeIdentifiers must
   * have type() == SnowflakeIdentifier.Type.TABLE.
   *
   * @param scope The scope in which to list, which may be ROOT, a DATABASE, or a SCHEMA.
   */
  List<SnowflakeIdentifier> listIcebergTables(SnowflakeIdentifier scope);

  /**
   * Returns Snowflake-level metadata containing locations to more detailed metadata.
   *
   * @param tableIdentifier The fully-qualified identifier that must be of type
   *     SnowflakeIdentifier.Type.TABLE.
   */
  SnowflakeTableMetadata loadTableMetadata(SnowflakeIdentifier tableIdentifier);
}

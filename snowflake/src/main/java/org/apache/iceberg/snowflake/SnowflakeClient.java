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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;

/**
 * This interface abstracts out the underlying communication protocols for contacting Snowflake to
 * obtain the various resource representations defined under "entities". Classes using this
 * interface should minimize assumptions about whether an underlying client uses e.g. REST, JDBC or
 * other underlying libraries/protocols.
 */
public interface SnowflakeClient extends Closeable {
  List<SnowflakeSchema> listSchemas(Namespace namespace);

  List<SnowflakeTable> listIcebergTables(Namespace namespace);

  SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier);

  @Override
  void close();
}

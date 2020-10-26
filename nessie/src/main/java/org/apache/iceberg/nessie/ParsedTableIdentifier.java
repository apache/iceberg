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

package org.apache.iceberg.nessie;

import com.dremio.nessie.client.NessieClient;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;

public class ParsedTableIdentifier {

  private final TableIdentifier tableIdentifier;
  private final Instant timestamp;
  private final String reference;

  /**
   * container class to hold all options in a Nessie table name.
   */
  public ParsedTableIdentifier(TableIdentifier tableIdentifier, Instant timestamp, String reference) {
    this.tableIdentifier = tableIdentifier;
    this.timestamp = timestamp;
    this.reference = reference;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getReference() {
    return reference;
  }


  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static ParsedTableIdentifier getParsedTableIdentifier(String path, Map<String, String> properties) {
    // I am assuming tables can't have @ or # symbols
    if (path.split("@").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one branch in %s", path));
    }
    if (path.split("#").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one timestamp in %s", path));
    }

    if (path.contains("@") && path.contains("#")) {
      throw new IllegalArgumentException("Currently we don't support referencing by timestamp, # is not allowed in " +
          "the table name");
    }

    if (path.contains("@")) {
      String[] tableRef = path.split("@");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      return new ParsedTableIdentifier(identifier, null, tableRef[1]);
    }

    if (path.contains("#")) {
      throw new IllegalArgumentException("Currently we don't support referencing by timestamp, # is not allowed in " +
          "the table name");
    }

    TableIdentifier identifier = TableIdentifier.parse(path);
    String reference = properties.get(NessieClient.CONF_NESSIE_REF);
    return new ParsedTableIdentifier(identifier, null, reference);
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static ParsedTableIdentifier getParsedTableIdentifier(TableIdentifier path, Map<String, String> properties) {
    ParsedTableIdentifier pti = getParsedTableIdentifier(path.name(), properties);
    return new ParsedTableIdentifier(TableIdentifier.of(path.namespace(), pti.getTableIdentifier().name()),
        pti.getTimestamp(),
        pti.getReference());
  }
}

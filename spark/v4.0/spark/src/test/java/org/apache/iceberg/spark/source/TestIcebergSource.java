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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class TestIcebergSource extends IcebergSource {
  @Override
  public String shortName() {
    return "iceberg-test";
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    TableIdentifier ti = TableIdentifier.parse(options.get("iceberg.table.name"));
    return Identifier.of(ti.namespace().levels(), ti.name());
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return SparkSession.active().sessionState().catalogManager().currentCatalog().name();
  }
}

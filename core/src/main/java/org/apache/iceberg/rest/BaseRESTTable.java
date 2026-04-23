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
package org.apache.iceberg.rest;

import java.util.Optional;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SupportsReadRestrictions;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;

/**
 * BaseTable specialization for tables loaded via a REST catalog. Carries the per-principal {@link
 * ReadRestrictions} that the REST server may have attached to the load response and advertises the
 * capability via {@link SupportsReadRestrictions}.
 *
 * <p>Used by {@link RESTSessionCatalog} for REST loadTable paths that do not use server-side scan
 * planning. {@link RESTTable} extends this class to add scan-planning. Non-REST catalogs (Hadoop,
 * Hive, Glue, JDBC, Nessie, etc.) construct {@link BaseTable} directly and do not advertise the
 * capability — they have no pathway to produce a {@link ReadRestrictions}.
 */
public class BaseRESTTable extends BaseTable implements SupportsReadRestrictions {
  private final ReadRestrictions readRestrictions;

  public BaseRESTTable(
      TableOperations ops,
      String name,
      MetricsReporter reporter,
      ReadRestrictions readRestrictions) {
    super(ops, name, reporter);
    this.readRestrictions = readRestrictions;
  }

  @Override
  public Optional<ReadRestrictions> readRestrictions() {
    return Optional.ofNullable(readRestrictions).filter(r -> !r.isEmpty());
  }
}

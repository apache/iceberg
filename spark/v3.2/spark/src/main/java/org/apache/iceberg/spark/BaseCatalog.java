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

package org.apache.iceberg.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.spark.procedures.ProcedureProvider;
import org.apache.iceberg.spark.procedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseCatalog implements StagingTableCatalog, ProcedureCatalog, SupportsNamespaces {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCatalog.class);

  private final AtomicReference<Map<Identifier, Supplier<ProcedureBuilder>>> procedureBuilders = new AtomicReference<>(
      Collections.emptyMap());

  protected void initializeProcedures(String catalogName, CaseInsensitiveStringMap options,
      boolean forSessionCatalog) {
    Map<Identifier, Supplier<ProcedureBuilder>> procedureBuilders = new HashMap<>();
    ServiceLoader<ProcedureProvider> loader = ServiceLoader.load(ProcedureProvider.class);
    for (ProcedureProvider procedureProvider : loader) {
      Map<String, Supplier<ProcedureBuilder>> provided = procedureProvider.getProcedureBuilders(
          catalogName, options, forSessionCatalog);
      if (provided != null && !provided.isEmpty()) {
        Namespace providerNamespace = procedureProvider.getNamespace();
        LOG.debug("Adding procedures {} in namespace {} from provider {} ({})",
            String.join(", ", provided.keySet()), providerNamespace,
            procedureProvider.getName(), procedureProvider.getDescription());
        String[] namespace = lowerCaseArray(providerNamespace.levels());
        for (Entry<String, Supplier<ProcedureBuilder>> proc : provided.entrySet()) {
          Identifier ident = Identifier.of(namespace, proc.getKey());
          if (procedureBuilders.put(ident, proc.getValue()) != null) {
            throw new IllegalArgumentException(
                String.format("Procedure identifier %s is used more than once.", ident));
          }
        }
      } else {
        LOG.debug("Ignoring procedure provider {} ({}) as it returned no procedures",
            lowerCase(procedureProvider.getName()), procedureProvider.getDescription());
      }
    }

    // In case initialize() get called again clear the already known procedure builders.
    this.procedureBuilders.set(Collections.unmodifiableMap(procedureBuilders));
  }

  @Override
  public Procedure loadProcedure(Identifier ident) throws NoSuchProcedureException {
    Supplier<ProcedureBuilder> builder = procedureBuilders.get().get(
        Identifier.of(lowerCaseArray(ident.namespace()), lowerCase(ident.name())));
    if (builder != null) {
      return builder.get().withTableCatalog(this).build();
    }
    throw new NoSuchProcedureException(ident);
  }

  private static String lowerCase(String s) {
    return s.toLowerCase(Locale.ROOT);
  }

  private static String[] lowerCaseArray(String[] namespace) {
    return Arrays.stream(namespace).map(BaseCatalog::lowerCase).toArray(String[]::new);
  }
}

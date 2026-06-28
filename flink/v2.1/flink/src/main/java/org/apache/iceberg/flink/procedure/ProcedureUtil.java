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
package org.apache.iceberg.flink.procedure;

import java.util.Map;
import java.util.ServiceLoader;
import org.apache.flink.table.procedures.Procedure;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Util class to provide specific implementation for the given procedure tag. Implementations of the
 * {@link ProcedureService} interface are loaded through Java's {@link ServiceLoader}.
 */
public class ProcedureUtil {

  private ProcedureUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  private static final Map<String, ProcedureService> REGISTRY = Maps.newHashMap();

  static {
    ServiceLoader<ProcedureService> services = ServiceLoader.load(ProcedureService.class);
    for (ProcedureService service : services) {
      REGISTRY.put(service.procedureName(), service);
    }
  }

  public static Procedure getProcedure(String procedureName, Catalog catalog) {
    ProcedureService procedureService = REGISTRY.get(procedureName);
    if (procedureService == null) {
      throw new IllegalArgumentException("Unknown procedure: " + procedureName);
    }
    return procedureService.create(catalog);
  }
}

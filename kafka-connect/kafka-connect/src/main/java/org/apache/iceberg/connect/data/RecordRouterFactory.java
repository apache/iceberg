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
package org.apache.iceberg.connect.data;

import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.connect.IcebergSinkConfig;

class RecordRouterFactory {

  static RecordRouter create(IcebergSinkConfig config) {
    String routerClass = config.tablesRouterClass();

    if (routerClass != null) {
      return loadCustomRouter(routerClass, config);
    }

    if (config.dynamicTablesEnabled()) {
      DynamicRouter router = new DynamicRouter();
      router.configure(config);
      return router;
    }

    StaticRouter router = new StaticRouter();
    router.configure(config);
    return router;
  }

  private static RecordRouter loadCustomRouter(String className, IcebergSinkConfig config) {
    try {
      Class<?> clazz = DynClasses.builder().impl(className).build();
      RecordRouter router =
          (RecordRouter) DynConstructors.builder().hiddenImpl(clazz).build().newInstance();
      router.configure(config.originalProps());
      return router;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create RecordRouter from class: " + className, e);
    }
  }

  private RecordRouterFactory() {}
}

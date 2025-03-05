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
package org.apache.iceberg.connect;

import java.lang.reflect.Constructor;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.sink.SinkTaskContext;

class CommitterFactory {
  static Committer createCommitter(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    try {
      // Load class dynamically
      Class<?> clazz = config.committer();
      // Ensure it implements Committer interface
      if (!Committer.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(clazz.getName() + " does not implement Committer interface");
      }

      // Get all constructors and match with provided args
      for (Constructor<?> constructor : clazz.getConstructors()) {
        Class<?>[] paramTypes = constructor.getParameterTypes();

        if (matchesConstructor(paramTypes, catalog, config, context)) {
          return (Committer) constructor.newInstance(catalog, config, context);
        }
      }

      throw new IllegalArgumentException("No matching constructor found for class: " + clazz.getName());

    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate Committer", e);
    }
  }

  private static boolean matchesConstructor(Class<?>[] paramTypes, Object... args) {
    if (paramTypes.length != args.length) {
      return false;
    }
    for (int i = 0; i < paramTypes.length; i++) {
      if (!paramTypes[i].isAssignableFrom(args[i].getClass())) {
        return false;
      }
    }
    return true;
  }

  private CommitterFactory() {}
}

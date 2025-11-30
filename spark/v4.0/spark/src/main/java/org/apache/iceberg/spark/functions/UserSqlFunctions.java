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
package org.apache.iceberg.spark.functions;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Simple in-memory registry that tracks SQL UDF names registered via SparkUDFRegistrar so that the
 * Iceberg Spark function catalog can list them.
 *
 * <p>This is a POC-only facility; persistence and resolution are handled by Spark SQL.
 */
public final class UserSqlFunctions {

  private UserSqlFunctions() {}

  private static final Set<String> NAMES = new ConcurrentSkipListSet<>();

  public static void register(String name) {
    if (name != null) {
      NAMES.add(name.toLowerCase(Locale.ROOT));
    }
  }

  public static List<String> list() {
    return Collections.unmodifiableList(Lists.newArrayList(NAMES));
  }
}

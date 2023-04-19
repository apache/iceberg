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
package org.apache.iceberg.actions;

import org.apache.iceberg.Table;
import org.apache.iceberg.common.DynConstructors;
import org.apache.spark.sql.SparkSession;

/**
 * An API for interacting with actions in Spark.
 *
 * @deprecated since 0.12.0, used for supporting {@link RewriteDataFilesAction} in Spark 2.4 for
 *     backward compatibility. This implementation is no longer maintained, the new implementation
 *     is available with Spark 3
 */
@Deprecated
public class Actions {

  // Load the actual implementation of Actions via reflection to allow for differences
  // between the major Spark APIs while still defining the API in this class.
  private static final String IMPL_NAME = "SparkActions";
  private static DynConstructors.Ctor<Actions> implConstructor;

  private static String implClass() {
    return Actions.class.getPackage().getName() + "." + IMPL_NAME;
  }

  private static DynConstructors.Ctor<Actions> actionConstructor() {
    if (implConstructor == null) {
      String className = implClass();
      try {
        implConstructor =
            DynConstructors.builder()
                .hiddenImpl(className, SparkSession.class, Table.class)
                .buildChecked();
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            "Cannot find appropriate Actions implementation on the classpath.", e);
      }
    }
    return implConstructor;
  }

  private SparkSession spark;
  private Table table;

  protected Actions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  /**
   * @deprecated since 0.12.0, used for supporting {@link RewriteDataFilesAction} in Spark 2.4 for
   *     backward compatibility. This implementation is no longer maintained, the new implementation
   *     is available with Spark 3
   */
  @Deprecated
  public static Actions forTable(SparkSession spark, Table table) {
    return actionConstructor().newInstance(spark, table);
  }

  /**
   * @deprecated since 0.12.0, used for supporting {@link RewriteDataFilesAction} in Spark 2.4 for
   *     backward compatibility. This implementation is no longer maintained, the new implementation
   *     is available with Spark 3
   */
  @Deprecated
  public static Actions forTable(Table table) {
    return forTable(SparkSession.active(), table);
  }

  /**
   * @deprecated since 0.12.0, used for supporting {@link RewriteDataFilesAction} in Spark 2.4 for
   *     backward compatibility. This implementation is no longer maintained, the new implementation
   *     is available with Spark 3
   */
  @Deprecated
  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(spark, table);
  }

  protected SparkSession spark() {
    return spark;
  }

  protected Table table() {
    return table;
  }
}

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;

public class Actions {
  private static final String IMPL_NAME = "SparkActions";

  private static Class<Actions> implClass;
  private static Constructor<Actions> implConstructor;

  private SparkSession spark;
  private Table table;

  protected Actions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  private static Constructor actionConstructor() {
    if (implClass == null) {
      String className = Actions.class.getPackage().getName() + "." + IMPL_NAME;
      try {
        implClass = (Class<Actions>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Cannot use Actions if no Spark Module is on the classpath.", e);
      }
    }
    if (implConstructor == null) {
      try {
        implConstructor = implClass.getDeclaredConstructor(SparkSession.class, Table.class);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            "Cannot use Actions if SparkAction implementation does not have a constructor of (SparkSession, Table).",
            e);
      }
    }
    return implConstructor;
  }

  public static Actions forTable(SparkSession spark, Table table) {
    try {
      return (Actions) actionConstructor().newInstance(spark, table);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Cannot create new Actions, this most likely means there is a classpath issue.",
          e);
    }
  }

  public static Actions forTable(Table table) {
    return forTable(SparkSession.active(), table);
  }

  public RemoveOrphanFilesAction removeOrphanFiles() {
    return new RemoveOrphanFilesAction(spark, table);
  }

  public RewriteManifestsAction rewriteManifests() {
    return new RewriteManifestsAction(spark, table);
  }

  public RewriteDataFilesAction rewriteDataFiles() {
    return new RewriteDataFilesAction(spark, table);
  }

  public ExpireSnapshotsAction expireSnapshots() {
    return new ExpireSnapshotsAction(spark, table);
  }
}

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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.functions.IcebergVersionFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFunctionCatalog extends SparkTestBaseWithCatalog {
  private static final String[] EMPTY_NAMESPACE = new String[] {};
  private static final String[] SYSTEM_NAMESPACE = new String[] {"system"};
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};
  private static final String[] DB_NAMESPACE = new String[] {"db"};
  private final FunctionCatalog asFunctionCatalog;

  public TestFunctionCatalog() {
    this.asFunctionCatalog = castToFunctionCatalog(catalogName);
  }

  @Before
  public void createDefaultNamespace() {
    sql("CREATE NAMESPACE IF NOT EXISTS %s", catalogName + ".default");
  }

  @After
  public void dropDefaultNamespace() {
    sql("DROP NAMESPACE IF EXISTS %s", catalogName + ".default");
  }

  @Test
  public void testListFunctionsViaCatalog() throws NoSuchNamespaceException {
    Assertions.assertThat(asFunctionCatalog.listFunctions(EMPTY_NAMESPACE))
        .anyMatch(func -> "iceberg_version".equals(func.name()));

    Assertions.assertThat(asFunctionCatalog.listFunctions(SYSTEM_NAMESPACE))
        .anyMatch(func -> "iceberg_version".equals(func.name()));

    Assert.assertArrayEquals(
        "Listing functions in an existing namespace that's not system should not throw",
        new Identifier[0],
        asFunctionCatalog.listFunctions(DEFAULT_NAMESPACE));

    AssertHelpers.assertThrows(
        "Listing functions in a namespace that does not exist should throw",
        NoSuchNamespaceException.class,
        "Namespace 'db' not found",
        () -> asFunctionCatalog.listFunctions(DB_NAMESPACE));
  }

  @Test
  public void testLoadFunctions() throws NoSuchFunctionException {
    for (String[] namespace : ImmutableList.of(EMPTY_NAMESPACE, SYSTEM_NAMESPACE)) {
      Identifier identifier = Identifier.of(namespace, "iceberg_version");
      UnboundFunction func = asFunctionCatalog.loadFunction(identifier);

      Assertions.assertThat(func)
          .isNotNull()
          .isInstanceOf(UnboundFunction.class)
          .isExactlyInstanceOf(IcebergVersionFunction.class);
    }

    AssertHelpers.assertThrows(
        "Cannot load a function if it's not used with the system namespace or the empty namespace",
        NoSuchFunctionException.class,
        "Undefined function: default.iceberg_version",
        () -> asFunctionCatalog.loadFunction(Identifier.of(DEFAULT_NAMESPACE, "iceberg_version")));

    Identifier undefinedFunction = Identifier.of(SYSTEM_NAMESPACE, "undefined_function");
    AssertHelpers.assertThrows(
        "Cannot load a function that does not exist",
        NoSuchFunctionException.class,
        "Undefined function: system.undefined_function",
        () -> asFunctionCatalog.loadFunction(undefinedFunction));

    AssertHelpers.assertThrows(
        "Using an undefined function from SQL should fail analysis",
        AnalysisException.class,
        "Undefined function",
        () -> sql("SELECT undefined_function(1, 2)"));
  }

  @Test
  public void testCallingFunctionInSQLEndToEnd() {
    String buildVersion = IcebergBuild.version();

    Assert.assertEquals(
        "Should be able to use the Iceberg version function from the fully qualified system namespace",
        buildVersion,
        scalarSql("SELECT %s.system.iceberg_version()", catalogName));

    Assert.assertEquals(
        "Should be able to use the Iceberg version function when fully qualified without specifying a namespace",
        buildVersion,
        scalarSql("SELECT %s.iceberg_version()", catalogName));

    sql("USE %s", catalogName);

    Assert.assertEquals(
        "Should be able to call iceberg_version from system namespace without fully qualified name when using Iceberg catalog",
        buildVersion,
        scalarSql("SELECT system.iceberg_version()"));

    Assert.assertEquals(
        "Should be able to call iceberg_version from empty namespace without fully qualified name when using Iceberg catalog",
        buildVersion,
        scalarSql("SELECT iceberg_version()"));
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}

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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.functions.SparkFunctions;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.collection.JavaConverters;

@RunWith(Parameterized.class)
public class TestFunctionCatalog extends SparkCatalogTestBase {
  // TODO - Add tests for SparkCatalogConfig.SPARK once the `system` namespace is resolvable from the session catalog.
  @Parameterized.Parameters(name = "catalogConfig = {0}")
  public static Object[][] parameters() {
    return new Object[][]{
        {SparkCatalogConfig.HADOOP},
        {SparkCatalogConfig.HIVE}
    };
  }

  private static final Namespace NS = Namespace.of("db");
  private final String fullNamespace;
  private final FunctionCatalog asFunctionCatalog;

  public TestFunctionCatalog(SparkCatalogConfig catalogConfig) {
    super(catalogConfig);
    this.fullNamespace = ("spark_catalog".equals(catalogName) ? "" : catalogName + ".") + NS;
    this.asFunctionCatalog = castToFunctionCatalog(catalogName);
  }

  @Before
  public void createNamespace() {
    sql("CREATE NAMESPACE IF NOT EXISTS %s", fullNamespace);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP NAMESPACE IF EXISTS %s", fullNamespace);
  }

  @Test
  public void testLoadAndListFunctionsFromSystemNamespaces() throws NoSuchFunctionException, NoSuchNamespaceException {
    String[] namespace = {"system"};
    Identifier identifier = Identifier.of(new String[]{"system"}, "truncate");

    UnboundFunction truncateFunc = asFunctionCatalog.loadFunction(identifier);
    Assert.assertNotNull("truncate function should be loadable via the FunctionCatalog", truncateFunc);
    Identifier[] identifiers = asFunctionCatalog.listFunctions(namespace);
    Assert.assertTrue("Functions listed from the system namespace should not be empty",
        identifiers.length > 0);
    List<String> functionNames = Arrays.stream(identifiers).map(Identifier::name).collect(Collectors.toList());
    Assertions.assertThat(functionNames).hasSameElementsAs(SparkFunctions.list());

    ScalarFunction<Integer> boundTruncate = (ScalarFunction<Integer>) truncateFunc.bind(
        new StructType()
            .add("width", DataTypes.IntegerType)
            .add("value", DataTypes.IntegerType));

    Object width = Integer.valueOf(10);
    Object toTruncate = Integer.valueOf(9);
    Assert.assertEquals("Binding the truncate function from the function catalog should produce a usable function",
            Integer.valueOf(0),
            boundTruncate.produceResult(
                InternalRow.fromSeq(
                    JavaConverters.asScalaBufferConverter(ImmutableList.of(width, toTruncate)).asScala().toSeq())));
  }

  @Test
  public void testLoadFunctionsFromInvalidNamespace() {
    AssertHelpers.assertThrows(
        "Function Catalog functions should only be accessible from the system namespace",
        AnalysisException.class,
        "Undefined function",
        () -> sql("SELECT %s.truncate(1, 2)", fullNamespace)
    );
  }

  @Test
  public void testUndefinedFunction() {
    AssertHelpers.assertThrows(
        "Using an undefined function on a defined namespace should throw",
        AnalysisException.class,
        "Undefined function",
        () -> sql("SELECT system.undefined_function(1, 2)")
    );
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}

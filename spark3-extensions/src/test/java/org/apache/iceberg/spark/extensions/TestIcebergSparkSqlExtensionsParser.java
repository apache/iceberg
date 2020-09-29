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

package org.apache.iceberg.spark.extensions;

import java.math.BigDecimal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.CallArgument;
import org.apache.spark.sql.catalyst.plans.logical.CallStatement;
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument;
import org.apache.spark.sql.catalyst.plans.logical.PositionalArgument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConverters;

public class TestIcebergSparkSqlExtensionsParser {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;
  private static ParserInterface parser = null;

  @BeforeClass
  public static void startSpark() {
    TestIcebergSparkSqlExtensionsParser.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .getOrCreate();
    TestIcebergSparkSqlExtensionsParser.parser = spark.sessionState().sqlParser();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestIcebergSparkSqlExtensionsParser.spark;
    TestIcebergSparkSqlExtensionsParser.spark = null;
    TestIcebergSparkSqlExtensionsParser.parser = null;
    currentSpark.stop();
  }

  @Test
  public void testPositionalArgs() throws ParseException {
    CallStatement call = (CallStatement) parser.parsePlan("CALL c.n.func(1, '2', 3L, true, 1.0D, 9.0e1, 900e-1BD)");
    Assert.assertEquals(ImmutableList.of("c", "n", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(7, call.args().size());

    checkArg(call, 0, 1);
    checkArg(call, 1, "2");
    checkArg(call, 2, 3L);
    checkArg(call, 3, true);
    checkArg(call, 4, 1.0D);
    checkArg(call, 5, 9.0e1);
    checkArg(call, 6, new BigDecimal("900e-1"));
  }

  @Test
  public void testNamedArgs() throws ParseException {
    CallStatement call = (CallStatement) parser.parsePlan("CALL cat.system.func(c1 => 1, c2 => '2', c3 => true)");
    Assert.assertEquals(ImmutableList.of("cat", "system", "func"), JavaConverters.seqAsJavaList(call.name()));

    Assert.assertEquals(3, call.args().size());

    checkArg(call, 0, "c1", 1);
    checkArg(call, 1, "c2", "2");
    checkArg(call, 2, "c3", true);
  }

  private void checkArg(CallStatement call, int index, Object expectedValue) {
    checkArg(call, index, null, expectedValue);
  }

  private void checkArg(CallStatement call, int index, String expectedName, Object expectedValue) {
    if (expectedName != null) {
      NamedArgument arg = (NamedArgument) call.args().apply(index);
      Assert.assertEquals(expectedName, arg.name());
    } else {
      CallArgument arg = call.args().apply(index);
      Assert.assertTrue(arg instanceof PositionalArgument);
    }
    Assert.assertEquals(toSparkLiteral(expectedValue), call.args().apply(index).expr());
  }

  private Literal toSparkLiteral(Object value) {
    return Literal$.MODULE$.apply(value);
  }
}

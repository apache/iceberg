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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.expressions.Term;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

public class TestExtendedParser {

  private static SparkSession spark;
  private static final String SQL_PARSER_FIELD = "sqlParser";

  @BeforeAll
  public static void before() {
    spark = SparkSession.builder().master("local").appName("TestExtendedParser").getOrCreate();
  }

  @AfterAll
  public static void after() {
    if (spark != null) {
      spark.stop();
    }
  }

  /**
   * Tests that the Iceberg extended SQL parser can correctly parse a sort order string and return
   * the expected RawOrderField.
   *
   * @throws Exception if reflection access fails
   */
  @Test
  public void testParseSortOrderWithRealIcebergExtendedParser() throws Exception {
    ParserInterface origParser = null;
    Class<?> clazz = spark.sessionState().getClass();
    while (clazz != null && origParser == null) {
      try {
        Field parserField = clazz.getDeclaredField(SQL_PARSER_FIELD);
        parserField.setAccessible(true);
        origParser = (ParserInterface) parserField.get(spark.sessionState());
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    assertThat(origParser).isNotNull();

    IcebergSparkSqlExtensionsParser icebergParser = new IcebergSparkSqlExtensionsParser(origParser);

    setSessionStateParser(spark.sessionState(), icebergParser);

    List<ExtendedParser.RawOrderField> fields =
        ExtendedParser.parseSortOrder(spark, "id ASC NULLS FIRST");

    assertThat(fields).isNotEmpty();
    ExtendedParser.RawOrderField first = fields.get(0);
    assertThat(first.direction()).isEqualTo(SortDirection.ASC);
    assertThat(first.nullOrder()).isEqualTo(NullOrder.NULLS_FIRST);

    setSessionStateParser(spark.sessionState(), origParser);
  }

  /**
   * Tests that parseSortOrder can find and use an ExtendedParser that is wrapped inside another
   * ParserInterface implementation.
   *
   * @throws Exception if reflection access fails
   */
  @Test
  public void testParseSortOrderFindsNestedExtendedParser() throws Exception {
    ExtendedParser icebergParser = mock(ExtendedParser.class);

    ExtendedParser.RawOrderField field =
        new ExtendedParser.RawOrderField(
            mock(Term.class), SortDirection.ASC, NullOrder.NULLS_FIRST);
    List<ExtendedParser.RawOrderField> expected = Collections.singletonList(field);

    when(icebergParser.parseSortOrder("id ASC NULLS FIRST")).thenReturn(expected);

    ParserInterface wrapper = new WrapperParser(icebergParser);

    setSessionStateParser(spark.sessionState(), wrapper);

    List<ExtendedParser.RawOrderField> result =
        ExtendedParser.parseSortOrder(spark, "id ASC NULLS FIRST");
    assertThat(result).isSameAs(expected);

    verify(icebergParser).parseSortOrder("id ASC NULLS FIRST");
  }

  /**
   * Tests that parseSortOrder throws an exception if no ExtendedParser instance can be found in the
   * parser chain.
   *
   * @throws Exception if reflection access fails
   */
  @Test
  public void testParseSortOrderThrowsWhenNoExtendedParserFound() throws Exception {
    ParserInterface dummy = mock(ParserInterface.class);
    setSessionStateParser(spark.sessionState(), dummy);

    assertThatThrownBy(() -> ExtendedParser.parseSortOrder(spark, "id ASC"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Iceberg ExtendedParser");
  }

  private static void setSessionStateParser(Object sessionState, ParserInterface parser)
      throws Exception {
    Class<?> clazz = sessionState.getClass();
    Field targetField = null;
    while (clazz != null && targetField == null) {
      try {
        targetField = clazz.getDeclaredField(SQL_PARSER_FIELD);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    if (targetField == null) {
      throw new IllegalStateException(
          "No suitable sqlParser field found in sessionState class hierarchy!");
    }
    targetField.setAccessible(true);
    targetField.set(sessionState, parser);
  }

  private static class WrapperParser implements ParserInterface {
    private final ParserInterface delegate;

    WrapperParser(ParserInterface delegate) {
      this.delegate = delegate;
    }

    public ParserInterface getDelegate() {
      return delegate;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public Expression parseExpression(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public LogicalPlan parseQuery(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public StructType parseRoutineParam(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public StructType parseTableSchema(String sqlText) throws ParseException {
      return null;
    }

    @Override
    public DataType parseDataType(String sqlText) throws ParseException {
      return null;
    }
  }
}

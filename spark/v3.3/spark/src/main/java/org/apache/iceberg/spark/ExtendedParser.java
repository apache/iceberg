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

import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.expressions.Term;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;

public interface ExtendedParser extends ParserInterface {
  String ICEBERG_SPARK_SQL_EXTENSIONS_PARSER_CLASS =
      "org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser";
  String PARSE_SORT_ORDER_METHOD = "parseSortOrder";

  class RawOrderField {
    private final Term term;
    private final SortDirection direction;
    private final NullOrder nullOrder;

    public RawOrderField(Term term, SortDirection direction, NullOrder nullOrder) {
      this.term = term;
      this.direction = direction;
      this.nullOrder = nullOrder;
    }

    public Term term() {
      return term;
    }

    public SortDirection direction() {
      return direction;
    }

    public NullOrder nullOrder() {
      return nullOrder;
    }
  }

  static List<RawOrderField> parseSortOrder(SparkSession spark, String orderString) {
    if (spark.sessionState().sqlParser() instanceof ExtendedParser) {
      ExtendedParser parser = (ExtendedParser) spark.sessionState().sqlParser();
      try {
        return parser.parseSortOrder(orderString);
      } catch (AnalysisException e) {
        throw new IllegalArgumentException(
            String.format("Unable to parse sortOrder: %s", orderString), e);
      }
    } else {
      try {
        Class<?> sqlExtensionsParserClass =
            DynClasses.builder().impl(ICEBERG_SPARK_SQL_EXTENSIONS_PARSER_CLASS).buildChecked();
        DynConstructors.Ctor<?> sqlExtensionsParserCtor =
            DynConstructors.builder()
                .loader(sqlExtensionsParserClass.getClassLoader())
                .impl(sqlExtensionsParserClass)
                .buildChecked();
        return DynMethods.builder(PARSE_SORT_ORDER_METHOD)
            .impl(sqlExtensionsParserClass, String.class)
            .buildChecked(sqlExtensionsParserCtor.newInstance())
            .invoke(orderString);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Cannot load IcebergSparkSqlExtensionsParser class", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            "Cannot initialize IcebergSparkSqlExtensionsParser ctor " + "or parseSortOrder method",
            e);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            String.format("Unable to parse sortOrder: %s", orderString), e);
      }
    }
  }

  List<RawOrderField> parseSortOrder(String orderString) throws AnalysisException;
}

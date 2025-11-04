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

import java.lang.reflect.Field;
import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.expressions.Term;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ExtendedParser extends ParserInterface {
  Logger LOG = LoggerFactory.getLogger(ExtendedParser.class);

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
    ExtendedParser extParser = findParser(spark.sessionState().sqlParser(), ExtendedParser.class);
    if (extParser != null) {
      try {
        return extParser.parseSortOrder(orderString);
      } catch (AnalysisException e) {
        throw new IllegalArgumentException(
            String.format("Unable to parse sortOrder: %s", orderString), e);
      }
    } else {
      throw new IllegalStateException(
          "Cannot parse order: parser is not an Iceberg ExtendedParser");
    }
  }

  private static <T> T findParser(ParserInterface parser, Class<T> clazz) {
    ParserInterface current = parser;
    while (current != null) {
      if (clazz.isInstance(current)) {
        return clazz.cast(current);
      }

      current = getNextDelegateParser(current);
    }

    return null;
  }

  private static ParserInterface getNextDelegateParser(ParserInterface parser) {
    try {
      Class<?> clazz = parser.getClass();
      while (clazz != null) {
        for (Field field : clazz.getDeclaredFields()) {
          field.setAccessible(true);
          Object value = field.get(parser);
          if (value instanceof ParserInterface && value != parser) {
            return (ParserInterface) value;
          }
        }
        clazz = clazz.getSuperclass();
      }
    } catch (Exception e) {
      LOG.warn("Failed to scan delegate parser in {}: ", parser.getClass().getName(), e);
    }

    return null;
  }

  List<RawOrderField> parseSortOrder(String orderString) throws AnalysisException;
}

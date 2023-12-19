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
package org.apache.iceberg.spark.source;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkAggregates;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;
import org.junit.jupiter.api.Test;

public class TestSparkAggregates {

  @Test
  public void testAggregates() {
    Map<String, String> attrMap = Maps.newHashMap();
    attrMap.put("id", "id");
    attrMap.put("`i.d`", "i.d");
    attrMap.put("`i``d`", "i`d");
    attrMap.put("`d`.b.`dd```", "d.b.dd`");
    attrMap.put("a.`aa```.c", "a.aa`.c");

    attrMap.forEach(
        (quoted, unquoted) -> {
          NamedReference namedReference = FieldReference.apply(quoted);

          Max max = new Max(namedReference);
          Expression expectedMax = Expressions.max(unquoted);
          Expression actualMax = SparkAggregates.convert(max);
          assertThat(expectedMax.toString()).as("Max must match").isEqualTo(String.valueOf(actualMax));

          Min min = new Min(namedReference);
          Expression expectedMin = Expressions.min(unquoted);
          Expression actualMin = SparkAggregates.convert(min);
          assertThat(expectedMin.toString()).as("Min must match").isEqualTo(String.valueOf(actualMin));

          Count count = new Count(namedReference, false);
          Expression expectedCount = Expressions.count(unquoted);
          Expression actualCount = SparkAggregates.convert(count);
          assertThat(expectedCount.toString()).as("Count must match").isEqualTo(String.valueOf(actualCount));

          Count countDistinct = new Count(namedReference, true);
          Expression convertedCountDistinct = SparkAggregates.convert(countDistinct);
          assertThat(convertedCountDistinct).as("Count Distinct is converted to null").isNull();

          CountStar countStar = new CountStar();
          Expression expectedCountStar = Expressions.countStar();
          Expression actualCountStar = SparkAggregates.convert(countStar);
          assertThat(expectedCountStar.toString()).as("CountStar must match").isEqualTo(String.valueOf(actualCountStar));
        });
  }
}

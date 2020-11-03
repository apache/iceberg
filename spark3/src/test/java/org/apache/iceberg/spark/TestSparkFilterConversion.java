/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkFilterConversion {
  @Test
  public void testNullValues() {
    Filter[] filters = new Filter[] {
        new EqualTo("a", null),
        new GreaterThan("a", null),
        new GreaterThanOrEqual("a", null),
        new LessThan("a", null),
        new LessThanOrEqual("a", null),
    };

    for (Filter filter : filters) {
      Assert.assertEquals("Null value should result in alwaysFalse",
          Expressions.alwaysFalse(), SparkFilters.convert(filter));
    }

    Assert.assertEquals("Null-safe equals should translate to isNull",
        Expression.Operation.IS_NULL, SparkFilters.convert(new EqualNullSafe("a", null)).op());
    Assert.assertEquals("Null-safe equals should translate to isNull",
        "a", ((UnboundPredicate<?>) SparkFilters.convert(new EqualNullSafe("a", null))).ref().name());
  }
}

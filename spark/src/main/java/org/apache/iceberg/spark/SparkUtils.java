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


import com.google.common.collect.Lists;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;

public final class SparkUtils {

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]");

  private SparkUtils() {}

  public static SparkSession getSparkSession() {
    return SparkSession.builder().getOrCreate();
  }

  public static Configuration getBaseConf() {
    return getSparkSession().sparkContext().hadoopConfiguration();
  }


  public static Transform[] toTransforms(PartitionSpec spec) {
    List<Transform> transforms = Lists.newArrayList();
    int numBuckets = 0;
    List<String> bucketColumns = Lists.newArrayList();

    for (PartitionField f : spec.fields()) {
      Matcher widthMatcher = HAS_WIDTH.matcher(f.transform().toString());
      if (widthMatcher.matches()) {
        String name = widthMatcher.group(1);
        if (name.equalsIgnoreCase("truncate")) {
          throw new UnsupportedOperationException("Spark doesn't support truncate transform");

        } else if (name.equalsIgnoreCase("bucket")) {
          numBuckets = Integer.parseInt(widthMatcher.group(2));
          bucketColumns.add(spec.schema().findColumnName(f.sourceId()));

        } else if (f.transform().toString().equalsIgnoreCase("identity")) {
          transforms.add(Expressions.identity(spec.schema().findColumnName(f.sourceId())));

        } else if (f.transform().toString().equalsIgnoreCase("year")) {
          transforms.add(Expressions.years(spec.schema().findColumnName(f.sourceId())));

        } else if (f.transform().toString().equalsIgnoreCase("month")) {
          transforms.add(Expressions.months(spec.schema().findColumnName(f.sourceId())));

        } else if (f.transform().toString().equalsIgnoreCase("day")) {
          transforms.add(Expressions.days(spec.schema().findColumnName(f.sourceId())));

        } else if (f.transform().toString().equalsIgnoreCase("hour")) {
          transforms.add(Expressions.hours(spec.schema().findColumnName(f.sourceId())));

        } else {
          throw new UnsupportedOperationException("Spark doesn't support transform " + f.transform());
        }
      }
    }

    if (!bucketColumns.isEmpty()) {
      transforms.add(Expressions.bucket(numBuckets, bucketColumns.toArray(new String[0])));
    }

    return transforms.toArray(new Transform[0]);
  }
}

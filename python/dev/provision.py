# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

spark = SparkSession.builder.getOrCreate()

print("Create database")

spark.sql(
    """
  CREATE DATABASE IF NOT EXISTS default;
"""
)

spark.sql(
    """
  use default;
"""
)

spark.sql(
    """
  DROP TABLE IF EXISTS test_null_nan;
"""
)

spark.sql(
    """
  CREATE TABLE test_null_nan
  USING iceberg
  AS SELECT
    1            AS idx,
    float('NaN') AS col_numeric
UNION ALL SELECT
    2            AS idx,
    null         AS col_numeric
UNION ALL SELECT
    3            AS idx,
    1            AS col_numeric
"""
)

spark.sql(
    """
  DROP TABLE IF EXISTS test_null_nan_rewritten;
"""
)

spark.sql(
    """
  CREATE TABLE test_null_nan_rewritten
  USING iceberg
  AS SELECT * FROM test_null_nan
"""
)

spark.sql(
    """
  DROP TABLE IF EXISTS test_limit;
"""
)

spark.sql(
    """
    CREATE TABLE test_limit
    USING iceberg
      AS SELECT
          1            AS idx
      UNION ALL SELECT
          2            AS idx
      UNION ALL SELECT
          3            AS idx
      UNION ALL SELECT
          4            AS idx
      UNION ALL SELECT
          5            AS idx
      UNION ALL SELECT
          6            AS idx
      UNION ALL SELECT
          7            AS idx
      UNION ALL SELECT
          8            AS idx
      UNION ALL SELECT
          9            AS idx
      UNION ALL SELECT
          10           AS idx
    """
)

spark.sql(
    """
  DROP TABLE IF EXISTS test_deletes;
"""
)

spark.sql(
    """
  CREATE TABLE test_deletes
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read'
  )
  AS SELECT
    1       AS idx,
    True    AS deleted
UNION ALL SELECT
    2       AS idx,
    False   AS deleted;
"""
)

spark.sql(
    """
  DELETE FROM test_deletes WHERE deleted = True;
"""
)

all_types_dataframe = (
    spark.range(0, 5, 1, 5)
    .withColumnRenamed("id", "longCol")
    .withColumn("intCol", expr("CAST(longCol AS INT)"))
    .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
    .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
    .withColumn("dateCol", date_add(current_date(), 1))
    .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
    .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
    .withColumn("booleanCol", expr("longCol > 5"))
    .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
    .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
    .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
    .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
    .withColumn("mapCol", expr("MAP(longCol, decimalCol)"))
    .withColumn("arrayCol", expr("ARRAY(longCol)"))
    .withColumn("structCol", expr("STRUCT(mapCol, arrayCol)"))
)

all_types_dataframe.writeTo("default.test_all_types").tableProperty("format-version", "2").partitionedBy(
    "intCol"
).createOrReplace()

while True:
    time.sleep(1)

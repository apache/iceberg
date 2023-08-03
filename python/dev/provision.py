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

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

spark = SparkSession.builder.getOrCreate()

spark.sql(
    """
  CREATE DATABASE IF NOT EXISTS default;
"""
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_null_nan
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
  CREATE OR REPLACE TABLE default.test_null_nan_rewritten
  USING iceberg
  AS SELECT * FROM default.test_null_nan
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.test_limit as
  SELECT * LATERAL VIEW explode(ARRAY(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) AS idx;
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE default.test_positional_mor_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

# Partitioning is not really needed, but there is a bug:
# https://github.com/apache/iceberg/pull/7685
spark.sql(
    """
    ALTER TABLE default.test_positional_mor_deletes ADD PARTITION FIELD years(dt) AS dt_years
"""
)

spark.sql(
    """
INSERT INTO default.test_positional_mor_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(
    """
DELETE FROM default.test_positional_mor_deletes WHERE number = 9
"""
)

spark.sql(
    """
  CREATE OR REPLACE TABLE default.test_positional_mor_double_deletes (
    dt     date,
    number integer,
    letter string
  )
  USING iceberg
  TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
  );
"""
)

# Partitioning is not really needed, but there is a bug:
# https://github.com/apache/iceberg/pull/7685
spark.sql(
    """
    ALTER TABLE default.test_positional_mor_double_deletes ADD PARTITION FIELD years(dt) AS dt_years
"""
)

spark.sql(
    """
INSERT INTO default.test_positional_mor_double_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(
    """
    DELETE FROM default.test_positional_mor_double_deletes WHERE number = 9
"""
)

spark.sql(
    """
    DELETE FROM default.test_positional_mor_double_deletes WHERE letter == 'f'
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

for table_name, partition in [
    ("test_partitioned_by_identity", "ts"),
    ("test_partitioned_by_years", "years(dt)"),
    ("test_partitioned_by_months", "months(dt)"),
    ("test_partitioned_by_days", "days(ts)"),
    ("test_partitioned_by_hours", "hours(ts)"),
    ("test_partitioned_by_truncate", "truncate(1, letter)"),
    ("test_partitioned_by_bucket", "bucket(16, number)"),
]:
    spark.sql(
        f"""
      CREATE OR REPLACE TABLE default.{table_name} (
        dt     date,
        ts     timestamp,
        number integer,
        letter string
      )
      USING iceberg;
    """
    )

    spark.sql(f"ALTER TABLE default.{table_name} ADD PARTITION FIELD {partition}")

    spark.sql(
        f"""
    INSERT INTO default.{table_name}
    VALUES
        (CAST('2022-03-01' AS date), CAST('2022-03-01 01:22:00' AS timestamp), 1, 'a'),
        (CAST('2022-03-02' AS date), CAST('2022-03-02 02:22:00' AS timestamp), 2, 'b'),
        (CAST('2022-03-03' AS date), CAST('2022-03-03 03:22:00' AS timestamp), 3, 'c'),
        (CAST('2022-03-04' AS date), CAST('2022-03-04 04:22:00' AS timestamp), 4, 'd'),
        (CAST('2023-03-05' AS date), CAST('2023-03-05 05:22:00' AS timestamp), 5, 'e'),
        (CAST('2023-03-06' AS date), CAST('2023-03-06 06:22:00' AS timestamp), 6, 'f'),
        (CAST('2023-03-07' AS date), CAST('2023-03-07 07:22:00' AS timestamp), 7, 'g'),
        (CAST('2023-03-08' AS date), CAST('2023-03-08 08:22:00' AS timestamp), 8, 'h'),
        (CAST('2023-03-09' AS date), CAST('2023-03-09 09:22:00' AS timestamp), 9, 'i'),
        (CAST('2023-03-10' AS date), CAST('2023-03-10 10:22:00' AS timestamp), 10, 'j'),
        (CAST('2023-03-11' AS date), CAST('2023-03-11 11:22:00' AS timestamp), 11, 'k'),
        (CAST('2023-03-12' AS date), CAST('2023-03-12 12:22:00' AS timestamp), 12, 'l');
    """
    )

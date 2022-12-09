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

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.getOrCreate()

print("Create database")

spark.sql(
    """
  CREATE DATABASE default;
"""
)

print("Create beers table")

df = spark.read.csv("/opt/spark/data/beers.csv", header=True)

df = (
    df.withColumn("abv", df.abv.cast(DoubleType()))
    .withColumn("ibu", df.ibu.cast(DoubleType()))
    .withColumn("beer_id", df.beer_id.cast(IntegerType()))
    .withColumn("brewery_id", df.brewery_id.cast(IntegerType()))
    .withColumn("ounces", df.ounces.cast(DoubleType()))
    # Inject a NaN which is nice for testing
    .withColumn("ibu", F.when(F.col("beer_id") == 2546, float("NaN")).otherwise(F.col("ibu")))
)

df.write.saveAsTable("default.beers")

print("Create breweries table")

df = spark.read.csv("/opt/spark/data/breweries.csv", header=True)

df = df.withColumn("brewery_id", df.brewery_id.cast(IntegerType()))

df.write.saveAsTable("default.breweries")

print("Done!")

while True:
    time.sleep(1)

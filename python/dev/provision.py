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
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder.getOrCreate()

spark.sql("""
  CREATE DATABASE IF NOT EXISTS default;
""")

df = spark.read.csv('/opt/spark/data/beers.csv', header=True)

df = (
    df.withColumn("abv", df.abv.cast(DoubleType()))
    .withColumn("ibu", df.ibu.cast(DoubleType()))
    .withColumn("beer_id", df.beer_id.cast(IntegerType()))
    .withColumn("brewery_id", df.brewery_id.cast(IntegerType()))
    .withColumn("ounces", df.ounces.cast(DoubleType()))
    # Inject a NaN which is nice for testing
    .withColumn("ibu", F.when(F.col("beer_id") == 2546, float('NaN')).otherwise(F.col("ibu")))
)

df.write.mode('overwrite').saveAsTable("default.beers")

spark.sql("""
CALL system.rewrite_data_files(table => 'default.beers', strategy => 'sort', sort_order => 'brewery_id')
""").show()

df = spark.read.csv('/opt/spark/data/breweries.csv', header=True)

df = (
    df.withColumn("brewery_id", df.brewery_id.cast(IntegerType()))
)

df.write.mode('overwrite').saveAsTable("default.breweries")

spark.sql("""
ALTER TABLE default.breweries ADD PARTITION FIELD state
""").show()

spark.sql("""
CALL system.rewrite_data_files('default.breweries')
""").show()

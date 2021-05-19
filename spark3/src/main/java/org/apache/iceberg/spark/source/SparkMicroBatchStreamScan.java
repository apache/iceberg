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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkMicroBatchStreamScan implements Scan, MicroBatchStream {

    private final JavaSparkContext sparkContext;
    private final Table table;
    private final boolean caseSensitive;
    private final boolean localityPreferred;
    private final Schema expectedSchema;
    private final List<Expression> filterExpressions;
    private final CaseInsensitiveStringMap options;

    private StructType readSchema = null;

    SparkMicroBatchStreamScan(SparkSession spark, Table table, boolean caseSensitive, Schema expectedSchema,
                              List<Expression> filters, CaseInsensitiveStringMap options) {
        this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.table = table;
        this.caseSensitive = caseSensitive;
        this.expectedSchema = expectedSchema;
        this.filterExpressions = filters != null ? filters : Collections.emptyList();
        this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        if (readSchema == null) {
            this.readSchema = SparkSchemaUtil.convert(expectedSchema);
        }
        return readSchema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return this;
    }

    @Override
    public Offset latestOffset() {
        return null;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        return new InputPartition[0];
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return null;
    }

    @Override
    public Offset initialOffset() {
        return null;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return StreamingOffset.fromJson(json);
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {

    }
}

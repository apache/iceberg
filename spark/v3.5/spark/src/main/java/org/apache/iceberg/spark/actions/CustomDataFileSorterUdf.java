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
package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructType;

/**
 * A UserDefinedFunction that is built specifically to translate its inputs into a {@link
 * org.apache.iceberg.DataFile} appearing object and pass that along to a runtime supplied Function.
 * The result of the execution of which must be a String.
 */
public class CustomDataFileSorterUdf implements UDF1<Row, String>, Serializable {
  // Supply how the DataFile should be interpreted from a raw Row.
  private Types.StructType dataFileType;
  private StructType dataFileSparkType;
  private Function<DataFile, String> clusteringFunction;

  public CustomDataFileSorterUdf(
      Function<DataFile, String> clusteringFunction,
      Types.StructType dataFileType,
      StructType dataFileSparkType) {
    this.dataFileType = dataFileType;
    this.dataFileSparkType = dataFileSparkType;
    this.clusteringFunction = clusteringFunction;
  }

  @Override
  public String call(Row dataFile) throws Exception {
    SparkDataFile wrapper = new SparkDataFile(dataFileType, dataFileSparkType);
    return this.clusteringFunction.apply(wrapper.wrap(dataFile));
  }
}

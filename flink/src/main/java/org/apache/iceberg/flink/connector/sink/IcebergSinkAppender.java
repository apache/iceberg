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

package org.apache.iceberg.flink.connector.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append Iceberg sink to DataStream
 *
 * @param <IN> input data type
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "checkstyle:HiddenField"})
public class IcebergSinkAppender<IN> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkAppender.class);

  private final Table table;
  private final Configuration config;
  private final String sinkName;
  private RecordSerializer<IN> serializer;
  private Integer writerParallelism;

  public IcebergSinkAppender(Table table, Configuration config, String sinkName) {
    this.table = table;
    this.config = config;
    this.sinkName = sinkName;
  }

  /**
   * Required.
   *
   * @param serializer Serialize input data type to Iceberg {@link Record}
   */
  public IcebergSinkAppender<IN> withSerializer(RecordSerializer<IN> serializer) {
    this.serializer = serializer;
    return this;
  }

  /**
   * Optional. Explicitly set the parallelism for Iceberg writer operator.
   * Otherwise, default job parallelism is used.
   */
  public IcebergSinkAppender<IN> withWriterParallelism(Integer writerParallelism) {
    this.writerParallelism = writerParallelism;
    return this;
  }

  /**
   * @param dataStream append sink to this DataStream
   */
  public DataStreamSink<FlinkDataFile> append(DataStream<IN> dataStream) {
    // TODO: need to return?
    IcebergWriter<IN> writer = new IcebergWriter<>(table, serializer, config);
    IcebergCommitter committer = new IcebergCommitter(table, config);

    final String writerId = sinkName + "-writer";
    SingleOutputStreamOperator<FlinkDataFile> writerStream = dataStream
        .transform(writerId, TypeInformation.of(FlinkDataFile.class), writer)  // IcebergWriter as stream operator
        .uid(writerId);
    if (null != writerParallelism && writerParallelism > 0) {
      LOG.info("Set Iceberg writer parallelism to {}", writerParallelism);
      writerStream.setParallelism(writerParallelism);
    }

    final String committerId = sinkName + "-committer";
    return writerStream
        .addSink(committer)  // IcebergCommitter as sink
        .name(committerId)
        .uid(committerId)
        .setParallelism(1);
  }
}

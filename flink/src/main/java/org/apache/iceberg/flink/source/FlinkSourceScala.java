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

package org.apache.iceberg.flink.source;

import java.util.Map;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.flink.util.IcebergScalaUtils;
import scala.collection.JavaConverters;

public class FlinkSourceScala {


  private FlinkSourceScala() {
  }

  /**
   * Initialize a {@link Builder} to read the data from iceberg table. Equivalent to {@link TableScan}. See more options
   * in {@link org.apache.iceberg.flink.source.ScanContext}.
   * <p>
   * The Source can be read static data in bounded mode. It can also continuously check the arrival of new data and read
   * records incrementally.
   * <ul>
   *   <li>Without startSnapshotId: Bounded</li>
   *   <li>With startSnapshotId and with endSnapshotId: Bounded</li>
   *   <li>With startSnapshotId (-1 means unbounded preceding) and Without endSnapshotId: Unbounded</li>
   * </ul>
   * <p>
   *
   * @return {@link Builder} to connect the iceberg table.
   */
  public static Builder forRowData() {
    return new Builder();
  }

  public static class Builder extends FlinkSource.Builder<Builder> {

    public Builder env(StreamExecutionEnvironment scalaEnv) {
      super.env(scalaEnv.getJavaEnv());
      return this;
    }

    public DataStream<RowData> buildAsScala() {
      return IcebergScalaUtils.asScalaStream(super.build());
    }
  }

  public static boolean isBounded(scala.collection.immutable.Map<String, String> propertiesScala) {
    return FlinkSource.isBounded((Map<String, String>) JavaConverters.mapAsJavaMapConverter(propertiesScala).asJava());
  }
}

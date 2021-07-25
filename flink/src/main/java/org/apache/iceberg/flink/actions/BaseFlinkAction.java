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

package org.apache.iceberg.flink.actions;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.Action;
import org.apache.iceberg.flink.source.MetadataTableSource;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;

abstract class BaseFlinkAction<ThisT, R> implements Action<ThisT, R> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFlinkAction.class);

  public static final String MAX_PARALLELISM = "max-parallelism";

  private final StreamExecutionEnvironment env;
  private final StreamTableEnvironment tEnv;
  private final Map<String, String> options = Maps.newHashMap();

  protected BaseFlinkAction(StreamExecutionEnvironment env) {
    this.env = env;

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    this.tEnv = StreamTableEnvironment.create(env, settings);
  }

  protected StreamTableEnvironment tableEnv() {
    return tEnv;
  }

  protected abstract ThisT self();

  @Override
  public ThisT option(String name, String value) {
    options.put(name, value);
    return self();
  }

  @Override
  public ThisT options(Map<String, String> newOptions) {
    options.putAll(newOptions);
    return self();
  }

  protected Map<String, String> options() {
    return options;
  }

  protected BaseTable newStaticTable(TableMetadata metadata, FileIO io) {
    String metadataFileLocation = metadata.metadataFileLocation();
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  protected org.apache.flink.table.api.Table buildValidDataFileTable(BaseTable table) {
    DataStream<RowData> allManifests = loadMetadataTable(table, MetadataTableType.ALL_MANIFESTS);
    DataStream<String> dataFilePaths = allManifests.flatMap(new ReadManifest(table.schema(), table.io()));
    return tEnv.fromDataStream(dataFilePaths);
  }

  protected org.apache.flink.table.api.Table buildManifestFileTable(BaseTable table) {
    DataStream<RowData> allManifests = loadMetadataTable(table, MetadataTableType.ALL_MANIFESTS);
    return tEnv.fromDataStream(allManifests).select($("path as file_path"));
  }

  protected org.apache.flink.table.api.Table buildManifestListTable(BaseTable table) {
    List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
    return tEnv.fromValues(manifestLists).select($("path as file_path"));
  }

  private DataStream<RowData> loadMetadataTable(BaseTable table, MetadataTableType type) {
    return MetadataTableSource.builder()
        .env(env)
        .tableName(table.name())
        .ops(table.operations())
        .type(type)
        .maxParallelism(PropertyUtil.propertyAsInt(options(), MAX_PARALLELISM, Integer.MAX_VALUE))
        .build();
  }

  private static class ReadManifest extends RichFlatMapFunction<RowData, String> {

    private final Schema schema;
    private final FileIO io;

    private ReadManifest(Schema schema, FileIO io) {
      this.schema = schema;
      this.io = io;
    }

    @Override
    public void flatMap(RowData row, Collector<String> out) throws Exception {
      ManifestFileBean manifestFileBean = new ManifestFileBean();
      manifestFileBean.setPath(row.getString(schema.aliasToId("path")).toString());

      try (CloseableIterator<String> iterator = ManifestFiles.readPaths(manifestFileBean, io).iterator()) {
        while (iterator.hasNext()) {
          out.collect(iterator.next());
        }
      } catch (Exception e) {
        LOG.error("Failed to read manifest file: " + manifestFileBean.getPath());
        throw e;
      }
    }
  }

}

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

package org.apache.iceberg.beam;

import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

import java.util.LinkedList;
import java.util.List;


public class IcebergIO {
  public static void write(TableIdentifier table, Schema schema, String hiveMetastoreUrl, WriteFilesResult<Void> resultFiles) {
    resultFiles.getPerDestinationOutputFilenames().apply(
        // TBD
        Combine.globally(new FileCombiner(table, schema, hiveMetastoreUrl))
    );
  }

  private static class FileCombiner extends Combine.CombineFn<KV<Void, String>, List<String>, Integer> {
    private final TableIdentifier tableIdentifier;
    private final Schema schema;
    private final String hiveMetastoreUrl;

    public FileCombiner(TableIdentifier table, Schema schema, String hiveMetastoreUrl) {
      this.tableIdentifier = table;
      this.schema = schema;
      this.hiveMetastoreUrl = hiveMetastoreUrl;
    }

    @Override
    public List<String> createAccumulator() {
      return new LinkedList<>();
    }

    @Override
    public List<String> addInput(List<String> mutableAccumulator, KV<Void, String> input) {
      mutableAccumulator.add(input.getValue());
      return mutableAccumulator;
    }

    @Override
    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
      return ImmutableList.copyOf(Iterables.concat(accumulators));
    }

    @Override
    public Integer extractOutput(List<String> accumulator) {
      try (HiveCatalog catalog = new HiveCatalog(
          HiveCatalog.DEFAULT_NAME,
          this.hiveMetastoreUrl,
          1,
          new Configuration()
      )) {
        Table table = catalog.loadTable(this.tableIdentifier);

        // In case the schema has changed
        if (table.schema() != this.schema) {
          table.updateSchema().unionByNameWith(this.schema).commit();
        }

        // Append the new files
        AppendFiles app = table.newAppend();
        // We need to get the statistics, not easy to get them through Beam
        for (String filename : accumulator) {
          app.appendFile(DataFiles.builder(table.spec())
              .withPath(filename)
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build());
        }
        app.commit();
      }

      // Maybe we can return something sensible here
      return 0;
    }
  }
}

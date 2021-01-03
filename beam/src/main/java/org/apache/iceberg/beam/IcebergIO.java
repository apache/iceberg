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

package org.apache.iceberg.beam;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class IcebergIO {

  private IcebergIO() {
  }

  static PCollection<Snapshot> write(TableIdentifier table,
                                     Schema schema,
                                     String hiveMetastoreUrl,
                                     WriteFilesResult<Void> resultFiles) {
    PCollection<KV<Void, String>> filenames = resultFiles
        .getPerDestinationOutputFilenames();

    FileCombiner combiner = new FileCombiner(table, schema, hiveMetastoreUrl);

    Combine.Globally<KV<Void, String>, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

    return filenames.apply(combined);
  }

  /**
   * Defines a custom {@link FileIO.Write.FileNaming} which will use the prefix and suffix supplied to create
   * a name based on the window, pane, number of shards, shard index, and compression. Removes
   * window when in the {@link GlobalWindow} and pane info when it is the only firing of the pane.
   */
  public static class HadoopCompatibleFilenamePolicy implements FileIO.Write.FileNaming, Serializable {
    private final String suffix;

    public HadoopCompatibleFilenamePolicy(String suffix) {
      this.suffix = suffix;
    }

    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
      // Replace the HH:mm:ss with HH-mm-ss, so we don't use any semicolon's
      final String format = "yyyy-MM-dd'T'HH-mm-ss";
      final DateTimeFormatter formatter = DateTimeFormat.forPattern(format);

      final StringBuilder res = new StringBuilder();
      if (window instanceof IntervalWindow) {
        if (res.length() > 0) {
          res.append("-");
        }
        final IntervalWindow iw = (IntervalWindow) window;
        res.append(iw.start().toString(formatter)).append("-").append(iw.end().toString(formatter));
      }
      boolean isOnlyFiring = pane.isFirst() && pane.isLast();
      if (!isOnlyFiring) {
        if (res.length() > 0) {
          res.append("-");
        }
        res.append(pane.getIndex());
      }
      if (res.length() > 0) {
        res.append("-");
      }
      final String numShardsStr = String.valueOf(numShards);
      // A trillion shards per window per pane ought to be enough for everybody.
      final DecimalFormat df =
          new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
      res.append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
      res.append(this.suffix);
      res.append(compression.getSuggestedSuffix());
      return res.toString();
    }
  }


  private static class FileCombiner extends Combine.CombineFn<KV<Void, String>, List<String>, Snapshot> {
    private final TableIdentifier tableIdentifier;
    private final Schema schema;
    private final String hiveMetastoreUrl;

    FileCombiner(TableIdentifier table, Schema schema, String hiveMetastoreUrl) {
      this.tableIdentifier = table;
      this.schema = schema;
      this.hiveMetastoreUrl = hiveMetastoreUrl;
    }

    @Override
    public List<String> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<String> addInput(List<String> mutableAccumulator, KV<Void, String> input) {
      mutableAccumulator.add(input.getValue());
      return mutableAccumulator;
    }

    @Override
    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
      Iterator<List<String>> itr = accumulators.iterator();

      if(itr.hasNext()) {
        List<String> first = itr.next();

        while(itr.hasNext()) {
          first.addAll(itr.next());
        }

        return first;
      } else {
        return new ArrayList<>();
      }
    }

    @Override
    public Snapshot extractOutput(List<String> accumulator) {
      try (HiveCatalog catalog = new HiveCatalog(
          HiveCatalog.DEFAULT_NAME,
          this.hiveMetastoreUrl,
          1,
          new Configuration()
      )) {
        Table table;
        try {
          table = catalog.loadTable(this.tableIdentifier);
        } catch (NoSuchTableException e) {
          // If it doesn't exist, we just create the table
          table = catalog.createTable(this.tableIdentifier, schema);
        }

        // Append the new files
        final AppendFiles app = table.newAppend();
        // We need to get the statistics, not easy to get them through Beam
        for (String filename : accumulator) {
          app.appendFile(DataFiles.builder(table.spec())
              .withPath(filename)
              .withFileSizeInBytes(1024)
              .withRecordCount(100)
              .build());
        }
        app.commit();

        return table.currentSnapshot();
      }
    }
  }

}

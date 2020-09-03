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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestProcessor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class DistributedManifestProcessor extends ManifestProcessor {
    transient private final SparkSession spark;
    private final Broadcast<FileIO> ioBroadcast;

    public DistributedManifestProcessor(SparkSession spark, FileIO io) {
        this.spark = spark;
        this.ioBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(io);
    }

  @Override
  public <T extends ContentFile<T>> Iterable<CloseableIterable<ManifestEntry<T>>> readManifests(
      Iterable<ManifestFile> fromIterable,
      BiFunction<ManifestFile, FileIO, CloseableIterable<ManifestEntry<T>>> reader) {
      List<Iterable<ManifestEntry<T>>> results =
          JavaSparkContext.fromSparkContext(spark.sparkContext())
              .parallelize(Lists.newArrayList(fromIterable))
              .map(file -> {
                CloseableIterable<ManifestEntry<T>> closeable = reader.apply(file, ioBroadcast.getValue());
                Iterable<ManifestEntry<T>> realizedEntries = Lists.newArrayList(CloseableIterable.transform(closeable,
                    ManifestEntry::copy));
                closeable.close();
                return realizedEntries;
              }).collect();
    return results.stream().map(list -> CloseableIterable.withNoopClose(list)).collect(Collectors.toList());
  }
}

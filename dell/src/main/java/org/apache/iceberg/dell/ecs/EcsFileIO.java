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
package org.apache.iceberg.dell.ecs;

import com.emc.object.s3.S3Client;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.dell.DellClientFactories;
import org.apache.iceberg.dell.DellClientFactory;
import org.apache.iceberg.dell.DellProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO implementation backed by Dell EMC ECS.
 *
 * <p>Locations used must follow the conventions for ECS URIs (e.g. ecs://bucket/path...). URIs with
 * schemes s3, s3a, s3n, https are also treated as ECS object paths. Using this FileIO with other
 * schemes will result in {@link org.apache.iceberg.exceptions.ValidationException}.
 */
public class EcsFileIO implements FileIO {

  private static final Logger LOG = LoggerFactory.getLogger(EcsFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private SerializableSupplier<S3Client> s3;
  private DellProperties dellProperties;
  private DellClientFactory dellClientFactory;
  private transient volatile S3Client client;
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private MetricsContext metrics = MetricsContext.nullMetrics();

  @Override
  public InputFile newInputFile(String path) {
    return EcsInputFile.fromLocation(path, client(), dellProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return EcsOutputFile.fromLocation(path, client(), dellProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    EcsURI uri = new EcsURI(path);

    client().deleteObject(uri.bucket(), uri.name());
  }

  private S3Client client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = s3.get();
        }
      }
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.dellProperties = new DellProperties(properties);
    this.dellClientFactory = DellClientFactories.from(properties);
    this.s3 = dellClientFactory::ecsS3;

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("ecs");
      context.initialize(properties);
      this.metrics = context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics",
          DEFAULT_METRICS_IMPL,
          e);
    }
  }

  @Override
  public void close() {
    // handles concurrent calls to close()
    if (isResourceClosed.compareAndSet(false, true)) {
      client.destroy();
    }
  }
}

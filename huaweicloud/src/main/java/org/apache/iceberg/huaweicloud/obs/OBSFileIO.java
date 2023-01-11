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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.IObsClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.huaweicloud.HuaweicloudClientFactories;
import org.apache.iceberg.huaweicloud.HuaweicloudClientFactory;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OBSFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(OBSFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private SerializableSupplier<IObsClient> obs;
  private HuaweicloudProperties huaweicloudProperties;
  private transient volatile IObsClient client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  /**
   * No-arg constructor to load the FileIO dynamically.
   *
   * <p>All fields are initialized by calling {@link OBSFileIO#initialize(Map)} later.
   */
  public OBSFileIO() {}

  /**
   * Constructor with custom obs supplier and default huaweicloud properties.
   *
   * <p>Calling {@link OBSFileIO#initialize(Map)} will overwrite information set in this
   * constructor.
   *
   * @param obs obs supplier
   */
  public OBSFileIO(SerializableSupplier<IObsClient> obs) {
    this.obs = obs;
    this.huaweicloudProperties = new HuaweicloudProperties();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new OBSInputFile(client(), new OBSURI(path), huaweicloudProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new OBSOutputFile(client(), new OBSURI(path), huaweicloudProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    OBSURI location = new OBSURI(path);
    client().deleteObject(location.bucket(), location.key());
  }

  private IObsClient client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = obs.get();
        }
      }
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    HuaweicloudClientFactory factory = HuaweicloudClientFactories.from(properties);
    this.huaweicloudProperties = factory.huaweicloudProperties();
    this.obs = factory::newOBSClient;

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("obs");
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
      if (client != null) {
        try {
          client.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close OBSClient", e);
        }
      }
    }
  }
}

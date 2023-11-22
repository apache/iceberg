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
package org.apache.iceberg.tencentcloud.cos;

import com.qcloud.cos.COS;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(CosFileIO.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";

  private SerializableSupplier<COS> cosSupplier;
  private CosProperties cosProperties;
  private transient volatile COS client;
  private MetricsContext metrics = MetricsContext.nullMetrics();
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);
  private transient StackTraceElement[] createStack;

  public CosFileIO() {}

  public CosFileIO(SerializableSupplier<COS> cosSupplier, CosProperties properties) {
    this.cosSupplier = cosSupplier;
    this.cosProperties = properties;
    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new CosInputFile(client(), new CosURI(path), cosProperties, metrics);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new CosOutputFile(client(), new CosURI(path), cosProperties, metrics);
  }

  @Override
  public void deleteFile(String path) {
    CosURI location = new CosURI(path);
    client().deleteObject(location.bucket(), location.key());
  }

  private COS client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = cosSupplier.get();
        }
      }
    }
    return client;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    CosClientFactory factory = CosClientFactories.from(properties);
    this.cosProperties = factory.cosProperties();
    this.cosSupplier = factory::newCosClient;

    // Report Hadoop metrics if Hadoop is available
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance("cos");
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
    isResourceClosed.compareAndSet(false, true);
    // cos does not have a close api.
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!isResourceClosed.get()) {
      close();

      if (null != createStack) {
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed CosFileIO instance created by:\n\t{}", trace);
      }
    }
  }
}

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
package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.aws.metrics.DefaultSqsMetricsReporterAwsClientFactory;
import org.apache.iceberg.aws.metrics.SqsMetricsReporterAwsClientFactory;
import org.apache.iceberg.aws.metrics.SqsMetricsReporterProperties;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.util.PropertyUtil;

public class SqsMetricsReporterAwsClientFactories {

  private SqsMetricsReporterAwsClientFactories() {}

  /**
   * Attempts to load an AWS client factory class for SQS Metrics Reporter defined in the catalog
   * property {@link SqsMetricsReporterProperties#METRICS_REPORTER_SQS_CLIENT_FACTORY_IMPL}. If the
   * property wasn't set, fallback to {@link AwsClientFactories#from(Map) to intialize an AWS client
   * factory class}
   *
   * @param properties catalog properties
   * @return an instance of a factory class
   */
  @SuppressWarnings("unchecked")
  public static <T> T initialize(Map<String, String> properties) {
    String factoryImpl =
        PropertyUtil.propertyAsString(
            properties,
            SqsMetricsReporterProperties.METRICS_REPORTER_SQS_CLIENT_FACTORY_IMPL,
            DefaultSqsMetricsReporterAwsClientFactory.class.getName());
    return (T) loadClientFactory(factoryImpl, properties);
  }

  private static SqsMetricsReporterAwsClientFactory loadClientFactory(
      String impl, Map<String, String> properties) {
    DynConstructors.Ctor<SqsMetricsReporterAwsClientFactory> ctor;
    try {
      ctor =
          DynConstructors.builder(SqsMetricsReporterAwsClientFactory.class)
              .loader(SqsMetricsReporterAwsClientFactories.class.getClassLoader())
              .hiddenImpl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SqsMetricsReporterAwsClientFactory, missing no-arg constructor: %s",
              impl),
          e);
    }

    SqsMetricsReporterAwsClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize SqsMetricsReporterAwsClientFactory, %s does not implement SqsMetricsReporterAwsClientFactory.",
              impl),
          e);
    }

    factory.initialize(properties);
    return factory;
  }
}

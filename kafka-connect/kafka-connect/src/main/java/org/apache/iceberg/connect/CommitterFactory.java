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
package org.apache.iceberg.connect;

import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.connect.channel.CommitterImpl;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link Committer} instances based on configuration.
 *
 * <p>This factory supports pluggable Committer implementations through the {@code
 * iceberg.committer.class} configuration property. If not specified, the default {@link
 * CommitterImpl} is used.
 *
 * <p>Custom committer implementations must:
 *
 * <ol>
 *   <li>Implement the {@link Committer} interface
 *   <li>Provide a public no-arg constructor
 * </ol>
 *
 * <p>Example configuration:
 *
 * <pre>
 * # Use default committer (no configuration needed)
 *
 * # Use custom committer
 * iceberg.committer.class=com.example.MyCustomCommitter
 *
 * # Pass properties to custom committer (accessible via config)
 * iceberg.committer.retry-count=5
 * iceberg.committer.batch-size=1000
 * </pre>
 *
 * @see DynConstructors
 */
class CommitterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterFactory.class);

  /**
   * Creates a Committer instance based on the provided configuration.
   *
   * <p>If {@code iceberg.committer.class} is configured, instantiates that class using {@link
   * DynConstructors}. Otherwise, returns the default {@link CommitterImpl}.
   *
   * @param config The sink configuration containing committer settings
   * @return A configured Committer instance
   * @throws ConfigException if the specified class cannot be loaded or instantiated
   */
  static Committer createCommitter(IcebergSinkConfig config) {
    String committerClass = config.committerClass();

    if (committerClass == null || committerClass.trim().isEmpty()) {
      LOG.info("Using default CommitterImpl");
      return new CommitterImpl();
    }

    LOG.info("Creating custom committer: {}", committerClass);
    return loadCommitter(committerClass.trim());
  }

  /**
   * Loads a Committer implementation
   *
   * @param className Fully-qualified class name of the Committer implementation
   * @return A new Committer instance
   * @throws ConfigException if the class cannot be loaded or does not have a no-arg constructor
   */
  private static Committer loadCommitter(String className) {
    Preconditions.checkNotNull(className, "Committer class name cannot be null");
    Preconditions.checkArgument(
        !className.trim().isEmpty(), "Committer class name cannot be empty");

    try {
      DynConstructors.Ctor<Committer> ctor =
          DynConstructors.builder(Committer.class).impl(className).build();

      Committer committer = ctor.newInstance();

      LOG.info(
          "Successfully loaded custom committer: {} ({})",
          className,
          committer.getClass().getSimpleName());

      return committer;

    } catch (ClassCastException e) {
      throw new ConfigException(
          String.format(
              "Class %s does not implement the Committer interface. "
                  + "Custom committers must implement org.apache.iceberg.connect.Committer",
              className),
          e);
    } catch (RuntimeException e) {
      // DynConstructors wraps errors in RuntimeException
      String message = e.getMessage();

      if (message != null && message.contains("ClassNotFoundException")) {
        throw new ConfigException(
            String.format(
                "Committer class not found: %s. "
                    + "Ensure the class is on the classpath and the fully-qualified name is correct.",
                className),
            e);
      } else if (message != null
          && message.contains("cannot be cast to class org.apache.iceberg.connect.Committer")) {
        throw new ConfigException(
            String.format(
                "Provided implementation %s must implement org.apache.iceberg.connect.Committer"
                    + "Details: %s",
                className, message),
            e);
      } else if (message != null && message.contains("Cannot find constructor")) {
        throw new ConfigException(
            String.format(
                "Cannot find no-arg constructor for committer class: %s. "
                    + "Custom committers must provide a public no-argument constructor. "
                    + "Details: %s",
                className, message),
            e);
      } else {
        throw new ConfigException(
            String.format(
                "Failed to instantiate committer class: %s. Error: %s", className, message),
            e);
      }
    }
  }

  private CommitterFactory() {}
}

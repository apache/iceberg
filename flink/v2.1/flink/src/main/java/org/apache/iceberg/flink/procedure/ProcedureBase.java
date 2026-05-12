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
package org.apache.iceberg.flink.procedure;

import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.util.TimeUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/** Base implementation for flink's {@link Procedure}. */
public abstract class ProcedureBase implements Procedure, ProcedureService {

  private Catalog catalog;

  public ProcedureBase withCatalog(Catalog workingCatalog) {
    this.catalog = workingCatalog;
    return this;
  }

  @Override
  public Procedure create(Catalog workingCatalog) {
    try {
      return this.getClass().getDeclaredConstructor().newInstance().withCatalog(workingCatalog);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Table table(String tableId) throws NoSuchTableException {
    return catalog.loadTable(TableIdentifier.parse(tableId));
  }

  protected String[] execute(ProcedureContext procedureContext, JobClient jobClient) {
    StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
    ReadableConfig conf = env.getConfiguration();
    return execute(jobClient, conf.get(TableConfigOptions.TABLE_DML_SYNC));
  }

  protected String[] execute(StreamExecutionEnvironment env, String defaultJobName)
      throws Exception {
    ReadableConfig conf = env.getConfiguration();
    String name = conf.getOptional(PipelineOptions.NAME).orElse(defaultJobName);
    return execute(env.executeAsync(name), conf.get(TableConfigOptions.TABLE_DML_SYNC));
  }

  private String[] execute(JobClient jobClient, boolean dmlSync) {
    String jobId = jobClient.getJobID().toString();
    if (dmlSync) {
      try {
        jobClient.getJobExecutionResult().get();
      } catch (Exception e) {
        throw new TableException(String.format("Failed to wait job '%s' finish", jobId), e);
      }
      return new String[] {"Success"};
    } else {
      return new String[] {"JobID=" + jobId};
    }
  }

  @Nullable
  protected static Duration toDuration(@Nullable String text) {
    if (text == null) {
      return null;
    }

    return TimeUtils.parseDuration(text);
  }
}

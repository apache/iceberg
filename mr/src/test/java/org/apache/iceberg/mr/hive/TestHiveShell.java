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

package org.apache.iceberg.mr.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.HiveServer2;
import org.apache.iceberg.common.DynMethods;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkState;

/**
 * Test class for running HiveQL queries, essentially acting like a Beeline shell in tests.
 *
 * It takes a metastore URL via conf, and spins up an HS2 instance which connects to it. The shell will only accept
 * queries if it has been previously initialized via {@link #start()}, and a session has been opened via
 * {@link #openSession()}. Prior to calling {@link #start()}, the shell should first be configured with props that apply
 * across all test cases by calling {@link #setHiveConfValue(String, String)} ()}. On the other hand, session-level conf
 * can be applied anytime via {@link #setHiveSessionValue(String, String)} ()}, once we've opened an active session.
 */
public class TestHiveShell {

  private static final DynMethods.StaticMethod FIND_FREE_PORT = DynMethods.builder("findFreePort")
          .impl("org.apache.hadoop.hive.metastore.utils.MetaStoreUtils")
          .impl("org.apache.hadoop.hive.metastore.MetaStoreUtils")
          .buildStatic();

  private final HiveServer2 hs2;
  private final HiveConf conf;
  private CLIService client;
  private HiveSession session;
  private boolean started;

  public TestHiveShell() {
    conf = initializeConf();
    hs2 = new HiveServer2();
  }

  public void setHiveConfValue(String key, String value) {
    checkState(!started, "TestHiveShell has already been started. Cannot set Hive conf anymore.");
    conf.set(key, value);
  }

  public void setHiveSessionValue(String key, String value) {
    checkState(session != null, "There is no open session for setting variables.");
    try {
      session.getSessionConf().set(key, value);
    } catch (Exception e) {
      throw new RuntimeException("Unable to set Hive session variable: ", e);
    }
  }

  public void start() {
    checkState(isNotBlank(conf.get(HiveConf.ConfVars.METASTOREURIS.varname)),
            "hive.metastore.uris must be supplied in config. TestHiveShell needs an external metastore to connect to.");
    hs2.init(conf);
    hs2.start();
    client = hs2.getServices().stream()
            .filter(CLIService.class::isInstance)
            .findFirst()
            .map(CLIService.class::cast)
            .get();
    started = true;
  }

  public void stop() {
    if (client != null) {
      client.stop();
    }
    hs2.stop();
    started = false;
  }

  public void openSession() {
    checkState(started, "You have to start TestHiveShell first, before opening a session.");
    try {
      SessionHandle sessionHandle = client.getSessionManager().openSession(
              CLIService.SERVER_VERSION, "", "", "127.0.0.1", Collections.emptyMap());
      session = client.getSessionManager().getSession(sessionHandle);
    } catch (Exception e) {
      throw new RuntimeException("Unable to open new Hive session: ", e);
    }
  }

  public void closeSession() {
    checkState(session != null, "There is no open session to be closed.");
    try {
      session.close();
      session = null;
    } catch (Exception e) {
      throw new RuntimeException("Unable to close Hive session: ", e);
    }
  }

  public List<Object[]> executeStatement(String statement) {
    checkState(session != null,
            "You have to start TestHiveShell and open a session first, before running a query.");
    try {
      OperationHandle handle = client.executeStatement(session.getSessionHandle(), statement, Collections.emptyMap());
      List<Object[]> resultSet = new ArrayList<>();
      if (handle.hasResultSet()) {
        RowSet rowSet;
        // keep fetching results until we can
        while ((rowSet = client.fetchResults(handle)) != null && rowSet.numRows() > 0) {
          for (Object[] row : rowSet) {
            resultSet.add(row.clone());
          }
        }
      }
      return resultSet;
    } catch (HiveSQLException e) {
      throw new IllegalArgumentException("Failed to execute Hive query '" + statement + "': " + e.getMessage(), e);
    }
  }

  public Configuration getHiveConf() {
    if (session != null) {
      return session.getHiveConf();
    } else {
      return conf;
    }
  }

  private HiveConf initializeConf() {
    HiveConf hiveConf = new HiveConf();

    // Use random port to enable running tests in parallel
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, FIND_FREE_PORT.invoke());
    // Disable the web UI
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, 0);

    // Switch off optimizers in order to contain the map reduction within this JVM
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESKEWJOIN, false);

    // Speed up test execution
    hiveConf.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);

    return hiveConf;
  }
}

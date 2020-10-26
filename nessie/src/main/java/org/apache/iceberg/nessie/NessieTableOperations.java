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

package org.apache.iceberg.nessie;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableIcebergTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

/**
 * Nessie implementation of Iceberg TableOperations.
 */
public class NessieTableOperations extends BaseMetastoreTableOperations {

  private static DynFields.StaticField<Object> sparkEnvMethod;
  private static DynFields.UnboundField<Object> sparkConfMethod;
  private static DynFields.UnboundField<Object> appIdMethod;
  private final Configuration conf;
  private final NessieClient client;
  private final ContentsKey key;
  private UpdateableReference reference;
  private IcebergTable table;
  private HadoopFileIO fileIO;

  /**
   * Create a nessie table operations given a table identifier.
   */
  public NessieTableOperations(
      Configuration conf,
      ContentsKey key,
      UpdateableReference reference,
      NessieClient client) {
    this.conf = conf;
    this.key = key;
    this.reference = reference;
    this.client = client;
  }

  @Override
  protected void doRefresh() {
    // break reference with parent (to avoid cross-over refresh)
    // TODO, confirm this is correct behavior.
    // reference = reference.copy();

    reference.refresh();
    String metadataLocation = null;
    try {
      Contents contents = client.getContentsApi().getContents(key, reference.getHash());
      this.table = contents.unwrap(IcebergTable.class)
          .orElseThrow(() ->
              new IllegalStateException("Cannot refresh iceberg table: " +
                  String.format("Nessie points to a non-Iceberg object for path: %s.", key)));
      metadataLocation = table.getMetadataLocation();
    } catch (NessieNotFoundException ex) {
      throw new NoSuchTableException(ex, "No such table %s", key);
    }
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    reference.checkMutable();

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      IcebergTable newTable = ImmutableIcebergTable.builder().metadataLocation(newMetadataLocation).build();
      client.getContentsApi().setContents(key,
                                          reference.getAsBranch().getName(),
                                          reference.getHash(),
                                          String.format("iceberg commit%s", applicationId()),
                                          newTable);
    } catch (NessieConflictException ex) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(ex, "Commit failed: Reference hash is out of date. " +
          (reference.isBranch() ? "Update the reference %s and try again" : "Can't commit to the tag %s"),
          reference.getName());
    } catch (NessieNotFoundException ex) {
      io().deleteFile(newMetadataLocation);
      throw new RuntimeException(String.format("Commit failed: Reference %s does not exist", reference.getName()), ex);
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new RuntimeException("Unexpected commit exception", e);
    }
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  /**
   * try and get a Spark application id if one exists.
   *
   * <p>
   *   We haven't figured out a general way to pass commit messages through to the Nessie committer yet.
   *   This is hacky but gets the job done until we can have a more complete commit/audit log.
   * </p>
   */
  private static String applicationId() {

    try {
      if (sparkConfMethod == null) {
        sparkEnvMethod = DynFields.builder().impl("org.apache.spark.SparkEnv", "get").buildStatic();
        sparkConfMethod = DynFields.builder().impl("org.apache.spark.SparkEnv", "conf").build();
        appIdMethod = DynFields.builder().impl("org.apache.spark.SparkConf", "getAppId").build();
      }
      Object sparkEnv = sparkEnvMethod.get();
      Object sparkConf = sparkConfMethod.bind(sparkEnv).get();
      return "\nspark.app.id= " + appIdMethod.bind(sparkConf).get();
    } catch (Exception e) {
      return "";
    }
  }

}

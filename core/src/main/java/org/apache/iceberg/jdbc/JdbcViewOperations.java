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
package org.apache.iceberg.jdbc;

import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JDBC implementation of Iceberg ViewOperations. */
public class JdbcViewOperations extends BaseViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcViewOperations.class);
  private final String catalogName;
  private final TableIdentifier viewIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool connections;
  private final Map<String, String> catalogProperties;

  protected JdbcViewOperations(
      JdbcClientPool dbConnPool,
      FileIO fileIO,
      String catalogName,
      TableIdentifier viewIdentifier,
      Map<String, String> catalogProperties) {
    this.catalogName = catalogName;
    this.viewIdentifier = viewIdentifier;
    this.fileIO = fileIO;
    this.connections = dbConnPool;
    this.catalogProperties = catalogProperties;
  }

  @Override
  protected void doRefresh() {
    Map<String, String> view;

    try {
      view = JdbcUtil.loadView(JdbcUtil.SchemaVersion.V1, connections, catalogName, viewIdentifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting view from catalog
      throw new UncheckedSQLException(
          e, "Failed to get view %s from catalog %s", viewIdentifier, catalogName);
    }

    if (view.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = view.get(JdbcTableOperations.METADATA_LOCATION_PROP);
    Preconditions.checkState(
        newMetadataLocation != null, "Invalid view %s: metadata location is null", viewIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    try {
      Map<String, String> view =
          JdbcUtil.loadView(JdbcUtil.SchemaVersion.V1, connections, catalogName, viewIdentifier);
      if (base != null) {
        validateMetadataLocation(view, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        LOG.debug("Committing existing view: {}", viewName());
        updateView(newMetadataLocation, oldMetadataLocation);
      } else {
        // view does not exist, create it
        LOG.debug("Committing new view: {}", viewName());
        createView(newMetadataLocation);
      }

    } catch (SQLIntegrityConstraintViolationException e) {
      if (currentMetadataLocation() == null) {
        throw new AlreadyExistsException(e, "View already exists: %s", viewIdentifier);
      } else {
        throw new UncheckedSQLException(e, "View already exists: %s", viewIdentifier);
      }

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Database Connection timeout");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Database Connection failed");
    } catch (DataTruncation e) {
      throw new UncheckedSQLException(e, "Database data truncation error");
    } catch (SQLWarning e) {
      throw new UncheckedSQLException(e, "Database warning");
    } catch (SQLException e) {
      // SQLite doesn't set SQLState or throw SQLIntegrityConstraintViolationException
      if (e.getMessage() != null && e.getMessage().contains("constraint failed")) {
        throw new AlreadyExistsException("View already exists: %s", viewIdentifier);
      }

      throw new UncheckedSQLException(e, "Unknown failure");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    }
  }

  @Override
  protected String viewName() {
    return viewIdentifier.toString();
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }

  private void validateMetadataLocation(Map<String, String> view, ViewMetadata base) {
    String catalogMetadataLocation = view.get(JdbcTableOperations.METADATA_LOCATION_PROP);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s: metadata location %s has changed from %s",
          viewIdentifier, baseMetadataLocation, catalogMetadataLocation);
    }
  }

  private void updateView(String newMetadataLocation, String oldMetadataLocation)
      throws SQLException, InterruptedException {
    int updatedRecords =
        JdbcUtil.updateView(
            connections, catalogName, viewIdentifier, newMetadataLocation, oldMetadataLocation);

    if (updatedRecords == 1) {
      LOG.debug("Successfully committed to existing view: {}", viewIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to update view %s from catalog %s", viewIdentifier, catalogName);
    }
  }

  private void createView(String newMetadataLocation) throws SQLException, InterruptedException {
    Namespace namespace = viewIdentifier.namespace();
    if (PropertyUtil.propertyAsBoolean(catalogProperties, JdbcUtil.STRICT_MODE_PROPERTY, false)
        && !JdbcUtil.namespaceExists(catalogName, connections, namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot create view %s in catalog %s. Namespace %s does not exist",
          viewIdentifier, catalogName, namespace);
    }

    if (JdbcUtil.tableExists(JdbcUtil.SchemaVersion.V1, catalogName, connections, viewIdentifier)) {
      throw new AlreadyExistsException("Table with same name already exists: %s", viewIdentifier);
    }

    if (JdbcUtil.viewExists(catalogName, connections, viewIdentifier)) {
      throw new AlreadyExistsException("View already exists: %s", viewIdentifier);
    }

    int insertRecord =
        JdbcUtil.doCommitCreateView(
            connections, catalogName, namespace, viewIdentifier, newMetadataLocation);

    if (insertRecord == 1) {
      LOG.debug("Successfully committed to new view: {}", viewIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to create view %s in catalog %s", viewIdentifier, catalogName);
    }
  }
}

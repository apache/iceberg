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

package org.apache.iceberg.aws.glue.util;

import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Table;
import org.apache.hadoop.hive.metastore.TableType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.iceberg.aws.glue.util.HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR;
import static org.apache.iceberg.aws.glue.util.ObjectTestUtils.getTestTable;

public class TestHiveTableValidator {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static final String EXPECTED_MESSAGE = "%s cannot be null";

  @Test
  public void testRequiredProperty_TableType() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "TableType"));
    Table tbl = getTestTable().withTableType(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_StorageDescriptor() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor"));
    Table tbl = getTestTable().withStorageDescriptor(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_InputFormat() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#InputFormat"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setInputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_OutputFormat() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#OutputFormat"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setOutputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_SerdeInfo() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#SerdeInfo"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setSerdeInfo(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_SerializationLibrary() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#SerdeInfo#SerializationLibrary"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().getSerdeInfo().setSerializationLibrary(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_ValidTable() {
    REQUIRED_PROPERTIES_VALIDATOR.validate(getTestTable());
  }

  @Test
  public void testValidate_ViewTableType() {
    Table tbl = getTestTable();
    tbl.setTableType(TableType.VIRTUAL_VIEW.name());
    tbl.getStorageDescriptor().getSerdeInfo().setSerializationLibrary(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testValidate_ExcludeStorageHandlerType() {
    Table tbl = getTestTable();
    tbl.getParameters().put(META_TABLE_STORAGE, "org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler");
    tbl.getStorageDescriptor().setInputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }
}

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import getpass
import logging
import socket
import time
from typing import List

from hmsclient import HMSClient
from hmsclient.genthrift.hive_metastore.ttypes import AlreadyExistsException, EnvironmentContext, FieldSchema, \
    LockComponent, LockLevel, LockRequest, LockResponse, LockState, LockType, NoSuchObjectException, \
    SerDeInfo, StorageDescriptor, Table, TException

from .hive_types import hive_types
from ..api import Schema
from ..api.types import Type
from ..core import BaseMetastoreTableOperations, TableMetadata
from ..core.filesystem import FileSystem, get_fs
from ..exceptions import AlreadyExistsException as IcebergAlreadyExistsException
from ..exceptions import CommitFailedException


class HiveTableOperations(BaseMetastoreTableOperations):

    def __init__(self: "HiveTableOperations", conf: dict, client: HMSClient, database: str, table: str) -> None:
        super(HiveTableOperations, self).__init__(conf)
        self._client = client
        self.database = database
        self.table = table
        try:
            self.refresh()
        except NoSuchObjectException:
            pass  # Object hasn't been created yet, can't refresh.

    def refresh(self: "HiveTableOperations") -> TableMetadata:
        with self._client as open_client:
            tbl_info = open_client.get_table(self.database, self.table)

        table_type = tbl_info.parameters.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)

        if not table_type or table_type.lower() != BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE:
            raise RuntimeError("Invalid table, not Iceberg: %s.%s" % (self.database,
                                                                      self.table))

        metadata_location = tbl_info.parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
        if not metadata_location:
            raise RuntimeError("Invalid table, missing metadata_location: %s.%s" % (self.database,
                                                                                    self.table))

        self.refresh_from_metadata_location(metadata_location)

        return self.current()

    def commit(self: "HiveTableOperations", base: TableMetadata, metadata: TableMetadata) -> None:
        new_metadata_location = self.write_new_metadata(metadata, self.version + 1)

        threw = True
        lock_id = None
        try:
            lock_id = self.acquire_lock()
            if base:
                with self._client as open_client:
                    tbl = open_client.get_table(self.database, self.table)
            else:
                current_time_millis = int(time.time())
                tbl = Table(self.table,
                            self.database,
                            getpass.getuser(),
                            current_time_millis // 1000,
                            current_time_millis // 1000,
                            sd=storage_descriptor(metadata),
                            tableType="EXTERNAL_TABLE",
                            parameters={'EXTERNAL': 'TRUE'})

            tbl.sd = storage_descriptor(metadata)
            metadata_location = tbl.parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, None)
            base_metadata_location = base.metadata_location if base else None
            if base_metadata_location != metadata_location:
                raise CommitFailedException(
                    "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
                    base_metadata_location, metadata_location, self.database, self.table)

            self.set_parameters(new_metadata_location, tbl)

            if base:
                with self._client as open_client:
                    env_context = EnvironmentContext(
                        {"DO_NOT_UPDATE_STATS": "true"}
                    )
                    open_client.alter_table_with_environment_context(self.database, self.table, tbl, env_context)

            else:
                with self._client as open_client:
                    open_client.create_table(tbl)
            threw = False
        except AlreadyExistsException:
            raise IcebergAlreadyExistsException("Table already exists: {}.{}".format(self.database, self.table))
        except TException as e:
            if e and "Table/View 'HIVE_LOCKS' does not exist" in str(e):
                raise Exception("""Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't
                                exist, this probably happened when using embedded metastore or doesn't create a
                                transactional meta table. To fix this, use an alternative metastore""", e)

            raise Exception("Metastore operation failed for {}.{}".format(self.database, self.table), e)
        finally:
            if threw:
                self.io().delete(new_metadata_location)
            self.unlock(lock_id)

    def set_parameters(self: "HiveTableOperations", new_metadata_location: str, tbl: Table) -> None:
        parameters = tbl.parameters

        if not parameters:
            parameters = dict()

        parameters[BaseMetastoreTableOperations.TABLE_TYPE_PROP] = \
            BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.upper()
        parameters[BaseMetastoreTableOperations.METADATA_LOCATION_PROP] = new_metadata_location

        if self.current_metadata_location and len(self.current_metadata_location) > 0:
            parameters[BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP] = self.current_metadata_location

        tbl.parameters = parameters

    def unlock(self: "HiveTableOperations", lock_id: int = None) -> None:
        if lock_id:
            try:
                with self._client as open_client:
                    open_client.unlock(LockResponse(lock_id))
            except Exception as e:
                logging.warning("Failed to unlock {}.{}".format(self.database, self.table), e)

    def acquire_lock(self: "HiveTableOperations") -> int:
        lock_component = LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, self.database, self.table)

        lock_request = LockRequest([lock_component], user=getpass.getuser(), hostname=socket.gethostname())
        with self._client as open_client:
            lock_response = open_client.lock(lock_request)

        state = lock_response.state
        lock_id = lock_response.lockid
        start = int(time.time())
        duration = 0
        timed_out = False
        while not timed_out and state == LockState.WAITING:
            with self._client as open_client:
                lock_response = open_client.check_lock(lock_response)
            state = lock_response.state

            duration = int(time.time()) - start
            if duration > 3 * 60 * 1000:
                timed_out = True
            else:
                time.sleep(0.05)

        if timed_out and state != LockState.ACQUIRED:
            raise CommitFailedException("Timed out after {} ms waiting for lock on {}.{}".format(duration,
                                                                                                 self.database,
                                                                                                 self.table))

        if state != LockState.ACQUIRED:
            raise CommitFailedException(
                "Could not acquire the lock on {}.{}, lock request ended in state {}".format(self.database, self.table,
                                                                                             state))
        return lock_id

    def io(self: "HiveTableOperations") -> FileSystem:
        return get_fs(self.base_location, self.conf)

    def close(self: "HiveTableOperations") -> None:
        self._client.close()


def storage_descriptor(metadata: TableMetadata) -> StorageDescriptor:
    ser_de_info = SerDeInfo(serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    return StorageDescriptor(columns(metadata.schema),
                             metadata.location,
                             "org.apache.hadoop.mapred.FileInputFormat",
                             "org.apache.hadoop.mapred.FileOutputFormat",
                             serdeInfo=ser_de_info)


def columns(schema: Schema) -> List[FieldSchema]:
    return [FieldSchema(col.name, convert_hive_type(col.type), "") for col in schema.columns()]


def convert_hive_type(col_type: Type) -> str:
    type_id = col_type.type_id
    hive_type_id = hive_types.get(type_id)  # type: ignore
    if hive_type_id:
        return hive_type_id
    raise NotImplementedError("Not yet implemented column type " + str(col_type))

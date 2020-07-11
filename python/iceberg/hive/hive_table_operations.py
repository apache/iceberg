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

from hmsclient.genthrift.hive_metastore.ttypes import AlreadyExistsException, LockResponse, NoSuchObjectException, \
    TException
from hmsclient.genthrift.hive_metastore.ttypes import EnvironmentContext, FieldSchema, LockComponent, LockLevel, \
    LockRequest, LockState, LockType, SerDeInfo, StorageDescriptor, Table
from iceberg.core import BaseMetastoreTableOperations
from iceberg.core.filesystem import get_fs
from iceberg.exceptions import AlreadyExistsException as IcebergAlreadyExistsException
from iceberg.exceptions import CommitFailedException


class HiveTableOperations(BaseMetastoreTableOperations):

    def __init__(self, conf, client, database, table):
        super(HiveTableOperations, self).__init__(conf)
        self._client = client
        self.database = database
        self.table = table
        try:
            self.refresh()
        except NoSuchObjectException:
            pass  # Object hasn't been created yet, can't refresh.

    def refresh(self):
        with self._client as open_client:
            tbl_info = open_client.get_table(self.database, self.table)

        table_type = tbl_info.parameters.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)

        if table_type is None or table_type.lower() != BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE:
            raise RuntimeError("Invalid table, not Iceberg: %s.%s" % (self.database,
                                                                      self.table))

        metadata_location = tbl_info.parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
        if metadata_location is None:
            raise RuntimeError("Invalid table, missing metadata_location: %s.%s" % (self.database,
                                                                                    self.table))

        self.refresh_from_metadata_location(metadata_location)

        return self.current()

    def commit(self, base, metadata):
        new_metadata_location = self.write_new_metadata(metadata, self.version + 1)

        threw = True
        lock_id = None
        try:
            lock_id = self.acquire_lock()
            if base is not None:
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
            base_metadata_location = base.metadataFileLocation() if base is not None else None
            if base_metadata_location != metadata_location:
                raise CommitFailedException(
                    "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
                    base_metadata_location, metadata_location, self.database, self.table)

            self.set_parameters(new_metadata_location, tbl)

            if base is not None:
                with self._client as open_client:
                    env_context = EnvironmentContext(
                        {"DO_NOT_UPDATE_STATS": "true"}
                    )
                    open_client.alter_table(self.database, self.table, tbl, env_context)

            else:
                with self._client as open_client:
                    open_client.create_table(tbl)
            threw = False
        except AlreadyExistsException:
            raise IcebergAlreadyExistsException("Table already exists: {}.{}".format(self.database, self.table))
        except TException as e:
            if e is not None and "Table/View 'HIVE_LOCKS' does not exist" in str(e):
                raise Exception("""Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't
                                exist, this probably happened when using embedded metastore or doesn't create a
                                transactional meta table. To fix this, use an alternative metastore""", e)

            raise Exception("Metastore operation failed for {}.{}".format(self.database, self.table), e)
        finally:
            if threw:
                self.io().delete(new_metadata_location)
            self.unlock(lock_id)

    def set_parameters(self, new_metadata_location, tbl):
        parameters = tbl.parameters

        if not parameters:
            parameters = dict()

        parameters[BaseMetastoreTableOperations.TABLE_TYPE_PROP] = \
            BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.upper()
        parameters[BaseMetastoreTableOperations.METADATA_LOCATION_PROP] = new_metadata_location

        if self.current_metadata_location is not None and len(self.current_metadata_location) > 0:
            parameters[BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP] = self.current_metadata_location

        tbl.parameters = parameters

    def unlock(self, lock_id):
        if lock_id:
            try:
                self.do_unlock(LockResponse(lock_id))
            except Exception as e:
                logging.warning("Failed to unlock {}.{}".format(self.database, self.table), e)

    def do_unlock(self, lock_id):
        with self._client as open_client:
            open_client.unlock(lock_id)

    def acquire_lock(self):
        lock_component = LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, self.database, self.table)

        lock_request = LockRequest([lock_component], user=getpass.getuser(), hostname=socket.gethostname())
        with self._client as open_client:
            lock_response = open_client.lock(lock_request)

        state = lock_response.state
        lock_id = lock_response.lockid
        start = int(time.time())
        duration = 0
        timeout = False
        while not timeout and state == LockState.WAITING:
            with self._client as open_client:
                lock_response = open_client.check_lock(lock_response)
            state = lock_response.state

            duration = int(time.time()) - start
            if duration > 3 * 60 * 1000:
                timeout = True
            else:
                time.sleep(0.05)

        if timeout and state != LockState.ACQUIRED:
            raise CommitFailedException("Timed out after {} ms waiting for lock on {}.{}".format(duration,
                                                                                                 self.database,
                                                                                                 self.table))

        if state != LockState.ACQUIRED:
            raise CommitFailedException(
                "Could not acquire the lock on {}.{}, lock request ended in state {}".format(self.database, self.table,
                                                                                             state))
        return lock_id

    def io(self):
        return get_fs(self.base_location, self.conf)

    def close(self):
        self._client.close()


def storage_descriptor(metadata):
    ser_de_info = SerDeInfo(serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    return StorageDescriptor(columns(metadata.schema),
                             metadata.location,
                             "org.apache.hadoop.mapred.FileInputFormat",
                             "org.apache.hadoop.mapred.FileOutputFormat",
                             serdeInfo=ser_de_info)


def columns(schema):
    return [FieldSchema(col.name, convert_hive_type(col.type), "") for col in schema.columns()]


def convert_hive_type(col_type):
    try:
        type_id = col_type.type_id.value['hive_name']
        if type_id is not None:
            return type_id
    except:  # NOQA
        raise NotImplementedError("Not yet implemented column type " + col_type)
    raise NotImplementedError("Not yet implemented column type " + col_type)

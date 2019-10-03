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


__all__ = ["AtomicInteger",
           "METAFLOW_ENABLED",
           "METAFLOW_ROOTDIR",
           "PackingIterator",
           "PLANNER_THREAD_POOL_SIZE_PROP",
           "SCAN_THREAD_POOL_ENABLED",
           "str_as_bool",
           "WORKER_THREAD_POOL_SIZE_PROP",
           ]

from .atomic_integer import AtomicInteger
from .bin_packing import PackingIterator

PLANNER_THREAD_POOL_SIZE_PROP = "iceberg.planner.num-threads"
WORKER_THREAD_POOL_SIZE_PROP = "iceberg.worker.num-threads"
SCAN_THREAD_POOL_ENABLED = "iceberg.scan.plan-in-worker-pool"
METAFLOW_ENABLED = "iceberg.s3.use-metaflow"
METAFLOW_ROOTDIR = "iceberg.s3.metaflow-root-dir"


def str_as_bool(str_var):
    return str_var is not None and str_var.lower() == "true"

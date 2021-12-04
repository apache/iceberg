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


from contextlib import contextmanager
import logging
import time

_logger = logging.getLogger(__name__)


@contextmanager
def profile(label, stats_dict=None):
    if stats_dict is None:
        _logger.debug('PROFILE: %s starting' % label)
    start = time.time()
    yield
    took = int((time.time() - start) * 1000)
    if stats_dict is None:
        _logger.debug('PROFILE: %s completed in %dms' % (label, took))
    else:
        stats_dict[label] = stats_dict.get(label, 0) + took

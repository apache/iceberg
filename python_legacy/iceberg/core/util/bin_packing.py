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


from copy import deepcopy


class PackingIterator(object):

    def __init__(self, items, target_weight, lookback, weight_func):
        self.items = deepcopy(items)
        self.target_weight = target_weight
        self.lookback = lookback
        self.weight_func = weight_func
        self.bins = list()

    def __iter__(self):
        return self

    def __next__(self):
        item_copy = list(self.items)
        i = 0
        for item in item_copy:
            weight = self.weight_func(item)
            curr_bin = PackingIterator.find(self.bins, weight)
            if curr_bin is not None:
                curr_bin.add(item, weight)
                self.items.pop(i)
                i -= 1
            else:
                curr_bin = Bin(self.target_weight)
                self.items.pop(i)
                i -= 1
                curr_bin.add(item, weight)
                self.bins.append(curr_bin)

                if len(self.bins) > self.lookback:
                    return list(self.bins.pop(0).items)
            i += 1

        if len(self.bins) == 0:
            raise StopIteration()

        return list(self.bins.pop(0).items)

    @staticmethod
    def find(bins, weight):
        for curr_bin in bins:
            if curr_bin.can_add(weight):
                return curr_bin


class Bin(object):

    def __init__(self, target_weight):
        self.bin_weight = 0
        self.target_weight = target_weight
        self.items = list()

    def can_add(self, weight):
        return self.bin_weight + weight <= self.target_weight

    def add(self, item, weight):
        self.bin_weight += weight
        self.items.append(item)

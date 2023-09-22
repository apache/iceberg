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

import random
from typing import List

import pytest

from pyiceberg.utils.bin_packing import PackingIterator


@pytest.mark.parametrize(
    "splits, lookback, split_size, open_cost",
    [
        ([random.randint(0, 64) for x in range(200)], 20, 128, 4),  # random splits
        ([], 20, 128, 4),  # no splits
        (
            [0] * 100 + [random.randint(0, 64) in range(10)] + [0] * 100,
            20,
            128,
            4,
        ),  # sparse
    ],
)
def test_bin_packing(splits: List[int], lookback: int, split_size: int, open_cost: int) -> None:
    def weight_func(x: int) -> int:
        return max(x, open_cost)

    item_list_sums: List[int] = [sum(item) for item in PackingIterator(splits, split_size, lookback, weight_func)]
    assert all(split_size >= item_sum >= 0 for item_sum in item_list_sums)


@pytest.mark.parametrize(
    "splits, target_weight, lookback, largest_bin_first, expected_lists",
    [
        (
            [36, 36, 36, 36, 73, 110, 128],
            128,
            2,
            True,
            [[110], [128], [36, 73], [36, 36, 36]],
        ),
        (
            [36, 36, 36, 36, 73, 110, 128],
            128,
            2,
            False,
            [[36, 36, 36], [36, 73], [110], [128]],
        ),
        (
            [64, 64, 128, 32, 32, 32, 32],
            128,
            1,
            True,
            [[64, 64], [128], [32, 32, 32, 32]],
        ),
        (
            [64, 64, 128, 32, 32, 32, 32],
            128,
            1,
            False,
            [[64, 64], [128], [32, 32, 32, 32]],
        ),
    ],
)
def test_bin_packing_lookback(
    splits: List[int], target_weight: int, lookback: int, largest_bin_first: bool, expected_lists: List[List[int]]
) -> None:
    def weight_func(x: int) -> int:
        return x

    assert list(PackingIterator(splits, target_weight, lookback, weight_func, largest_bin_first)) == expected_lists

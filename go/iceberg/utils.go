// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

import (
	"runtime/debug"

	"golang.org/x/exp/constraints"
)

var version string

func init() {
	version = "(unknown version)"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/iceberg/go/iceberg":
				version = dep.Version
			}
		}
	}
}

func Version() string { return version }

func max[T constraints.Ordered](vals ...T) T {
	if len(vals) == 0 {
		panic("can't call max with no arguments")
	}

	out := vals[0]
	for _, v := range vals[1:] {
		if v > out {
			out = v
		}
	}
	return out
}

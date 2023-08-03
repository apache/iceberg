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

package iceberg_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg/go/iceberg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemainingTypes(t *testing.T) {
	tests := []struct {
		expected string
		typ      iceberg.Type
	}{
		{"long", iceberg.PrimitiveTypes.Int64},
		{"double", iceberg.PrimitiveTypes.Float64},
		{"date", iceberg.PrimitiveTypes.Date},
		{"time", iceberg.PrimitiveTypes.Time},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp},
		{"timestamptz", iceberg.PrimitiveTypes.TimestampTz},
		{"uuid", iceberg.PrimitiveTypes.UUID},
		{"binary", iceberg.PrimitiveTypes.Binary},
		{"fixed[5]", iceberg.FixedTypeOf(5)},
		{"decimal(9, 4)", iceberg.DecimalTypeOf(9, 4)},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			var data = `{
				"id": 1,
				"name": "test",
				"type": "` + tt.expected + `",
				"required": false
			}`

			var n iceberg.NestedField
			require.NoError(t, json.Unmarshal([]byte(data), &n))
			assert.Truef(t, n.Type.Equals(tt.typ), "expected: %s\ngot: %s", tt.typ, n.Type)

			out, err := json.Marshal(n)
			require.NoError(t, err)
			assert.JSONEq(t, data, string(out))
		})
	}
}

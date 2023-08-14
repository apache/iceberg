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

func TestTypesBasic(t *testing.T) {
	tests := []struct {
		expected string
		typ      iceberg.Type
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool},
		{"int", iceberg.PrimitiveTypes.Int32},
		{"long", iceberg.PrimitiveTypes.Int64},
		{"float", iceberg.PrimitiveTypes.Float32},
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

func TestFixedType(t *testing.T) {
	typ := iceberg.FixedTypeOf(5)
	assert.Equal(t, 5, typ.Len())
	assert.Equal(t, "fixed[5]", typ.String())
	assert.True(t, typ.Equals(iceberg.FixedTypeOf(5)))
	assert.False(t, typ.Equals(iceberg.FixedTypeOf(6)))
}

func TestDecimalType(t *testing.T) {
	typ := iceberg.DecimalTypeOf(9, 2)
	assert.Equal(t, 9, typ.Precision())
	assert.Equal(t, 2, typ.Scale())
	assert.Equal(t, "decimal(9, 2)", typ.String())
	assert.True(t, typ.Equals(iceberg.DecimalTypeOf(9, 2)))
	assert.False(t, typ.Equals(iceberg.DecimalTypeOf(9, 3)))
}

func TestStructType(t *testing.T) {
	typ := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 1, Name: "required_field", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			{ID: 2, Name: "optional_field", Type: iceberg.FixedTypeOf(5), Required: false},
			{ID: 3, Name: "required_field", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 4, Name: "optional_field", Type: iceberg.DecimalTypeOf(8, 2), Required: false},
					{ID: 5, Name: "required_field", Type: iceberg.PrimitiveTypes.Int64, Required: false},
				},
			}, Required: false},
		},
	}

	assert.Len(t, typ.FieldList, 3)
	assert.False(t, typ.Equals(&iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 1, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int32, Required: true}}}))
	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.StructType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

func TestListType(t *testing.T) {
	typ := &iceberg.ListType{
		ElementID:       1,
		ElementRequired: false,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 2, Name: "required_field", Type: iceberg.DecimalTypeOf(8, 2), Required: true},
				{ID: 3, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			},
		},
	}

	assert.IsType(t, (*iceberg.StructType)(nil), typ.ElementField().Type)
	assert.Len(t, typ.ElementField().Type.(iceberg.NestedType).Fields(), 2)
	assert.Equal(t, 1, typ.ElementField().ID)
	assert.False(t, typ.Equals(&iceberg.ListType{
		ElementID:       1,
		ElementRequired: true,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 2, Name: "required_field", Type: iceberg.DecimalTypeOf(8, 2), Required: true},
			},
		},
	}))

	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.ListType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

func TestMapType(t *testing.T) {
	typ := &iceberg.MapType{
		KeyID:         1,
		KeyType:       iceberg.PrimitiveTypes.Float64,
		ValueID:       2,
		ValueType:     iceberg.PrimitiveTypes.UUID,
		ValueRequired: false,
	}

	assert.IsType(t, iceberg.PrimitiveTypes.Float64, typ.KeyField().Type)
	assert.Equal(t, 1, typ.KeyField().ID)
	assert.IsType(t, iceberg.PrimitiveTypes.UUID, typ.ValueField().Type)
	assert.Equal(t, 2, typ.ValueField().ID)
	assert.False(t, typ.Equals(&iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Int64,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.UUID, ValueRequired: false,
	}))
	assert.False(t, typ.Equals(&iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Float64,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: true,
	}))

	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.MapType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

var (
	NonParameterizedTypes = []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.Binary,
		iceberg.PrimitiveTypes.UUID,
	}
)

func TestNonParameterizedTypeEquality(t *testing.T) {
	for i, in := range NonParameterizedTypes {
		for j, check := range NonParameterizedTypes {
			if i == j {
				assert.Truef(t, in.Equals(check), "expected %s == %s", in, check)
			} else {
				assert.Falsef(t, in.Equals(check), "expected %s != %s", in, check)
			}
		}
	}
}

func TestTypeStrings(t *testing.T) {
	tests := []struct {
		typ iceberg.Type
		str string
	}{
		{iceberg.PrimitiveTypes.Bool, "boolean"},
		{iceberg.PrimitiveTypes.Int32, "int"},
		{iceberg.PrimitiveTypes.Int64, "long"},
		{iceberg.PrimitiveTypes.Float32, "float"},
		{iceberg.PrimitiveTypes.Float64, "double"},
		{iceberg.PrimitiveTypes.Date, "date"},
		{iceberg.PrimitiveTypes.Time, "time"},
		{iceberg.PrimitiveTypes.Timestamp, "timestamp"},
		{iceberg.PrimitiveTypes.TimestampTz, "timestamptz"},
		{iceberg.PrimitiveTypes.String, "string"},
		{iceberg.PrimitiveTypes.UUID, "uuid"},
		{iceberg.PrimitiveTypes.Binary, "binary"},
		{iceberg.FixedTypeOf(22), "fixed[22]"},
		{iceberg.DecimalTypeOf(19, 25), "decimal(19, 25)"},
		{&iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 1, Name: "required_field", Type: iceberg.PrimitiveTypes.String, Required: true, Doc: "this is a doc"},
				{ID: 2, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			},
		}, "struct<1: required_field: required string (this is a doc), 2: optional_field: required int>"},
		{&iceberg.ListType{
			ElementID: 22, Element: iceberg.PrimitiveTypes.String}, "list<string>"},
		{&iceberg.MapType{KeyID: 19, KeyType: iceberg.PrimitiveTypes.String, ValueID: 25, ValueType: iceberg.PrimitiveTypes.Float64},
			"map<string, double>"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.str, tt.typ.String())
	}
}

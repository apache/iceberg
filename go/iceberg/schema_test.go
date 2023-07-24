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
	"strings"
	"testing"

	"github.com/apache/iceberg/go/iceberg"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
		[]int{1},
		iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
		iceberg.NestedField{
			ID: 6, Name: "quux",
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
			Required: true},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					Fields: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					},
				},
				ElementRequired: true},
			Required: true},
		iceberg.NestedField{
			ID:   15,
			Name: "person",
			Type: &iceberg.StructType{
				Fields: []iceberg.NestedField{
					{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
			Required: false,
		},
	)

	tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1,
		[]int{2},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
	)
)

func TestSchemaToString(t *testing.T) {
	assert.Equal(t, 3, tableSchemaSimple.NumFields())
	assert.Equal(t, `table {
	1: foo: optional string
	2: bar: required int
	3: baz: optional boolean
}`, tableSchemaSimple.String())
}

func TestNestedFieldToString(t *testing.T) {
	tests := []struct {
		idx      int
		expected string
	}{
		{0, "1: foo: optional string"},
		{1, "2: bar: required int"},
		{2, "3: baz: optional boolean"},
		{3, "4: qux: required list<string>"},
		{4, "6: quux: required map<string, map<string, int>>"},
		{5, "11: location: required list<struct<latitude: float, longitude: float>>"},
		{6, "15: person: optional struct<name: string, age: int>"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tableSchemaNested.Field(tt.idx).String())
	}
}

func TestSchemaIndexByIDVisitor(t *testing.T) {
	index, err := iceberg.IndexByID(tableSchemaNested)
	require.NoError(t, err)

	assert.Equal(t, map[int]iceberg.NestedField{
		1: tableSchemaNested.Field(0),
		2: tableSchemaNested.Field(1),
		3: tableSchemaNested.Field(2),
		4: tableSchemaNested.Field(3),
		5: {ID: 5, Name: "element", Type: iceberg.PrimitiveTypes.String, Required: true},
		6: tableSchemaNested.Field(4),
		7: {ID: 7, Name: "key", Type: iceberg.PrimitiveTypes.String, Required: true},
		8: {ID: 8, Name: "value", Type: &iceberg.MapType{
			KeyID:         9,
			KeyType:       iceberg.PrimitiveTypes.String,
			ValueID:       10,
			ValueType:     iceberg.PrimitiveTypes.Int32,
			ValueRequired: true,
		}, Required: true},
		9:  {ID: 9, Name: "key", Type: iceberg.PrimitiveTypes.String, Required: true},
		10: {ID: 10, Name: "value", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		11: tableSchemaNested.Field(5),
		12: {ID: 12, Name: "element", Type: &iceberg.StructType{
			Fields: []iceberg.NestedField{
				{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
			},
		}, Required: true},
		13: {ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
		14: {ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
		15: tableSchemaNested.Field(6),
		16: {ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		17: {ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	}, index)
}

func TestSchemaIndexByName(t *testing.T) {
	index, err := iceberg.IndexByName(tableSchemaNested)
	require.NoError(t, err)

	assert.Equal(t, map[string]int{
		"foo":                        1,
		"bar":                        2,
		"baz":                        3,
		"qux":                        4,
		"qux.element":                5,
		"quux":                       6,
		"quux.key":                   7,
		"quux.value":                 8,
		"quux.value.key":             9,
		"quux.value.value":           10,
		"location":                   11,
		"location.element":           12,
		"location.element.latitude":  13,
		"location.element.longitude": 14,
		"location.latitude":          13,
		"location.longitude":         14,
		"person":                     15,
		"person.name":                16,
		"person.age":                 17,
	}, index)
}

func TestSchemaFindColumnName(t *testing.T) {
	tests := []struct {
		id   int
		name string
	}{
		{1, "foo"},
		{2, "bar"},
		{3, "baz"},
		{4, "qux"},
		{5, "qux.element"},
		{6, "quux"},
		{7, "quux.key"},
		{8, "quux.value"},
		{9, "quux.value.key"},
		{10, "quux.value.value"},
		{11, "location"},
		{12, "location.element"},
		{13, "location.element.latitude"},
		{14, "location.element.longitude"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := tableSchemaNested.FindColumnName(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.name, n)
		})
	}
}

func TestSchemaFindColumnNameIDNotFound(t *testing.T) {
	n, ok := tableSchemaNested.FindColumnName(99)
	assert.False(t, ok)
	assert.Empty(t, n)
}

func TestSchemaFindColumnNameByID(t *testing.T) {
	tests := []struct {
		id   int
		name string
	}{
		{1, "foo"},
		{2, "bar"},
		{3, "baz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := tableSchemaSimple.FindColumnName(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.name, n)
		})
	}
}

func TestSchemaFindFieldByID(t *testing.T) {
	index, err := iceberg.IndexByID(tableSchemaSimple)
	require.NoError(t, err)

	col1 := index[1]
	assert.Equal(t, 1, col1.ID)
	assert.Equal(t, iceberg.PrimitiveTypes.String, col1.Type)
	assert.False(t, col1.Required)

	col2 := index[2]
	assert.Equal(t, 2, col2.ID)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, col2.Type)
	assert.True(t, col2.Required)

	col3 := index[3]
	assert.Equal(t, 3, col3.ID)
	assert.Equal(t, iceberg.PrimitiveTypes.Bool, col3.Type)
	assert.False(t, col3.Required)
}

func TestFindFieldByIDUnknownField(t *testing.T) {
	index, err := iceberg.IndexByID(tableSchemaSimple)
	require.NoError(t, err)
	_, ok := index[4]
	assert.False(t, ok)
}

func TestSchemaFindField(t *testing.T) {
	tests := []iceberg.NestedField{
		{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
		{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			f, ok := tableSchemaSimple.FindFieldByID(tt.ID)
			assert.True(t, ok)
			assert.Equal(t, tt, f)

			f, ok = tableSchemaSimple.FindFieldByName(tt.Name)
			assert.True(t, ok)
			assert.Equal(t, tt, f)

			f, ok = tableSchemaSimple.FindFieldByNameCaseInsensitive(strings.ToUpper(tt.Name))
			assert.True(t, ok)
			assert.Equal(t, tt, f)
		})
	}
}

func TestSchemaFindType(t *testing.T) {
	_, ok := tableSchemaSimple.FindTypeByID(0)
	assert.False(t, ok)
	_, ok = tableSchemaSimple.FindTypeByName("FOOBAR")
	assert.False(t, ok)
	_, ok = tableSchemaSimple.FindTypeByNameCaseInsensitive("FOOBAR")
	assert.False(t, ok)

	tests := []struct {
		id   int
		name string
		typ  iceberg.Type
	}{
		{1, "foo", iceberg.PrimitiveTypes.String},
		{2, "bar", iceberg.PrimitiveTypes.Int32},
		{3, "baz", iceberg.PrimitiveTypes.Bool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, ok := tableSchemaSimple.FindTypeByID(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)

			typ, ok = tableSchemaSimple.FindTypeByName(tt.name)
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)

			typ, ok = tableSchemaSimple.FindTypeByNameCaseInsensitive(strings.ToUpper(tt.name))
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)
		})
	}
}

func TestSerializeSchema(t *testing.T) {
	data, err := json.Marshal(tableSchemaSimple)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"type": "struct",
		"fields": [
			{"id": 1, "name": "foo", "type": "string", "required": false},
			{"id": 2, "name": "bar", "type": "int", "required": true},
			{"id": 3, "name": "baz", "type": "boolean", "required": false}
		],
		"schema-id": 1,
		"identifier-field-ids": [2]
	}`, string(data))
}

func TestUnmarshalSchema(t *testing.T) {
	var schema iceberg.Schema
	require.NoError(t, json.Unmarshal([]byte(`{
		"type": "struct",
		"fields": [
			{"id": 1, "name": "foo", "type": "string", "required": false},
			{"id": 2, "name": "bar", "type": "int", "required": true},
			{"id": 3, "name": "baz", "type": "boolean", "required": false}
		],
		"schema-id": 1,
		"identifier-field-ids": [2]
	}`), &schema))

	assert.True(t, tableSchemaSimple.Equals(&schema))
}

func TestPruneColumnsString(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{1: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false})))
}

func TestPruneColumnsStringFull(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{1: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false})))
}

func TestPruneColumnsList(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{5: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
		}})))
}

func TestPruneColumnsListItself(t *testing.T) {
	_, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{4: {}}, false)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)

	assert.ErrorContains(t, err, "cannot explicitly project List or Map types, 4:qux of type list<string> was selected")
}

func TestPruneColumnsListFull(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{5: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
		}})))
}

func TestPruneColumnsMap(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{9: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsMapItself(t *testing.T) {
	_, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{6: {}}, false)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.ErrorContains(t, err, "cannot explicitly project List or Map types, 6:quux of type map<string, map<string, int>> was selected")
}

func TestPruneColumnsMapFull(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{9: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsMapKey(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{10: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsStruct(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{16: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       15,
			Name:     "person",
			Required: false,
			Type: &iceberg.StructType{
				Fields: []iceberg.NestedField{{
					ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false,
				}},
			},
		})))
}

func TestPruneColumnsStructFull(t *testing.T) {
	sc, err := iceberg.PruneColumns(tableSchemaNested, map[int]iceberg.Void{16: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       15,
			Name:     "person",
			Required: false,
			Type: &iceberg.StructType{
				Fields: []iceberg.NestedField{{
					ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false,
				}},
			},
		})))
}

func TestPruneColumnsEmptyStruct(t *testing.T) {
	schemaEmptyStruct := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 15, Name: "person", Type: &iceberg.StructType{}, Required: false,
	})

	sc, err := iceberg.PruneColumns(schemaEmptyStruct, map[int]iceberg.Void{15: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(0,
		iceberg.NestedField{
			ID: 15, Name: "person", Type: &iceberg.StructType{}, Required: false})))
}

func TestPruneColumnsEmptyStructFull(t *testing.T) {
	schemaEmptyStruct := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 15, Name: "person", Type: &iceberg.StructType{}, Required: false,
	})

	sc, err := iceberg.PruneColumns(schemaEmptyStruct, map[int]iceberg.Void{15: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(iceberg.NewSchema(0,
		iceberg.NestedField{
			ID: 15, Name: "person", Type: &iceberg.StructType{}, Required: false})))
}

func TestPruneColumnsStructInMap(t *testing.T) {
	nestedSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &iceberg.StructType{
					Fields: []iceberg.NestedField{
						{ID: 10, Name: "name", Type: iceberg.PrimitiveTypes.String},
						{ID: 11, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	sc, err := iceberg.PruneColumns(nestedSchema, map[int]iceberg.Void{11: {}}, false)
	require.NoError(t, err)

	expected := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &iceberg.StructType{
					Fields: []iceberg.NestedField{
						{ID: 11, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	assert.Truef(t, sc.Equals(expected), "expected: %s\ngot: %s", expected, sc)
}

func TestPruneColumnsStructInMapFull(t *testing.T) {
	nestedSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &iceberg.StructType{
					Fields: []iceberg.NestedField{
						{ID: 10, Name: "name", Type: iceberg.PrimitiveTypes.String},
						{ID: 11, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	sc, err := iceberg.PruneColumns(nestedSchema, map[int]iceberg.Void{11: {}}, true)
	require.NoError(t, err)

	expected := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &iceberg.StructType{
					Fields: []iceberg.NestedField{
						{ID: 11, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	assert.Truef(t, sc.Equals(expected), "expected: %s\ngot: %s", expected, sc)
}

func TestPruneColumnsSelectOriginalSchema(t *testing.T) {
	id := tableSchemaNested.HighestFieldID()
	selected := make(map[int]iceberg.Void)
	for i := 0; i < id; i++ {
		selected[i] = iceberg.Void{}
	}

	sc, err := iceberg.PruneColumns(tableSchemaNested, selected, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(tableSchemaNested))
}

func TestPruneNilSchema(t *testing.T) {
	_, err := iceberg.PruneColumns(nil, nil, true)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestSchemaRoundTrip(t *testing.T) {
	data, err := json.Marshal(tableSchemaNested)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"type": "struct",
		"schema-id": 1,
		"identifier-field-ids": [1],
		"fields": [
			{
				"type": "string",
				"id": 1,
				"name": "foo",
				"required": false
			},
			{
				"type": "int",
				"id": 2,
				"name": "bar",
				"required": true
			},
			{
				"type": "boolean",
				"id": 3,
				"name": "baz",
				"required": false
			},
			{
				"id": 4,
				"name": "qux",
				"required": true,
				"type": {
					"type": "list",
					"element-id": 5,
					"element-required": true,
					"element": "string"
				}
			},
			{
				"id": 6,
				"name": "quux",
				"required": true,
				"type": {
					"type": "map",
					"key-id": 7,
					"key": "string",
					"value-id": 8,
					"value": {
						"type": "map",
						"key-id": 9,
						"key": "string",
						"value-id": 10,
						"value": "int",
						"value-required": true
					},
					"value-required": true
				}
			},
			{
				"id": 11,
				"name": "location",
				"required": true,
				"type": {
					"type": "list",
					"element-id": 12,
					"element-required": true,
					"element": {
						"type": "struct",
						"fields": [
							{
								"id": 13,
								"name": "latitude",
								"type": "float",
								"required": false
							},
							{
								"id": 14,
								"name": "longitude",
								"type": "float",
								"required": false
							}
						]
					}
				}
			},
			{
				"id": 15,
				"name": "person",
				"required": false,
				"type": {
					"type": "struct",
					"fields": [
						{
							"id": 16,
							"name": "name",
							"type": "string",
							"required": false
						},
						{
							"id": 17,
							"name": "age",
							"type": "int",
							"required": true
						}
					]
				}
			}
		]
	}`, string(data))

	var sc iceberg.Schema
	require.NoError(t, json.Unmarshal(data, &sc))

	assert.Truef(t, tableSchemaNested.Equals(&sc), "expected: %s\ngot: %s", tableSchemaNested, &sc)
}

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

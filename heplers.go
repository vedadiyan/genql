// Copyright 2023 Pouya Vedadiyan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genql

import (
	"reflect"
	"strings"
)

func ValueOf(query *Query, current Map, any any) (any, error) {
	switch value := any.(type) {
	case ColumnName:
		{
			rs, err := ExecReader(current, string(value))
			if err != nil {
				// if errors.Is(err, KEY_NOT_FOUND) {
				// 	return nil, nil
				// }
				return nil, err
			}
			return rs, nil
		}
	case NeutalString:
		{
			return string(value), nil
		}
	case *float64:
		{
			if value == nil {
				return nil, nil
			}
			return *value, nil
		}
	default:
		{
			return value, nil
		}
	}
}

func AsType[T any](value any) (*T, error) {
	if value == nil {
		return nil, nil
	}

	switch t := value.(type) {
	case T:
		{
			return &t, nil
		}
	case *T:
		{
			return t, nil
		}

	}

	return new(T), INVALID_CAST
}

func AsArray(data any) ([]any, error) {
	switch data := data.(type) {
	case []any:
		{
			return data, nil
		}
	case Map:
		{
			return []any{data}, nil
		}
	default:
		{
			t := reflect.TypeOf(data)
			v := reflect.ValueOf(data)
			kind := t.Kind()
			switch kind {
			case reflect.Array, reflect.Slice:
				{
					len := v.Len()
					slice := make([]any, len)
					for i := 0; i < len; i++ {
						slice[i] = v.Index(i).Interface()
					}
					return slice, nil
				}
			}
		}
	}
	return nil, INVALID_TYPE
}

func IsImmediateFunction(name string) bool {
	for _, value := range immediateFunctions {
		if strings.ToLower(name) == value {
			return true
		}
	}
	return false
}

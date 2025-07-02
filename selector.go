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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// All type definitions
type (
	KeySelector              string
	TopLevelFunctionSelector string
	IndexType                int
	IndexSelector            struct {
		indexSelector int
		rangeSelector [2]int
		selectorType  IndexType
	}
	KeepDimension []*IndexSelector
	KeyType       int
	PipeSelector  struct {
		keySelector  string
		typeSelector string
	}
	TopLevelFunction map[string]func(data any) (any, error)
)

// Regex patterns
const (
	_FULLPATTERN  = `('[^']*'+|\<\-|\*|[\w]+|\[[^\[\]]*\]|\{[^\{\}]*\})`
	_ARRAYPATTERN = `\([^\)]*\)+|\w+`
	_PIPEPATTERN  = `('[^']*'+|\w+)(!?\|\w+)|\w+`
)

// IndexType enum
const (
	INDEX IndexType = iota
	RANGE
)

// KeyType enum
const (
	NONE KeyType = iota
	STRING
	NUMBER
	UNKNOWN
)

// Tokens
const (
	_LPAR      = '('
	_RPAR      = ')'
	_COL       = ':'
	_WITESPACE = ' '
	_PIPE      = '|'
	_LBRA      = '['
	_RBRA      = ']'
	_LCUR      = '{'
	_RCUR      = '}'
	_SQ        = '\''
)

// Keywords
const (
	_EACH  = "each"
	_BEGIN = "begin"
	_END   = "end"
)

// Arrow functions
const (
	_KEEP = "keep=>"
)

var (
	fullPattern       *regexp.Regexp
	arrayPattern      *regexp.Regexp
	pipePattern       *regexp.Regexp
	topLevelFunctions TopLevelFunction
)

func init() {
	fullPattern = regexp.MustCompile(_FULLPATTERN)
	arrayPattern = regexp.MustCompile(_ARRAYPATTERN)
	pipePattern = regexp.MustCompile(_PIPEPATTERN)
	RegisterTopLevelFunction("mix", Mix)
	RegisterTopLevelFunction("distinct", Distinct)
}

func NewIndex[T int | [2]int](value T) *IndexSelector {
	switch value := any(value).(type) {
	case int:
		{
			return &IndexSelector{
				indexSelector: value,
				selectorType:  INDEX,
			}
		}
	case [2]int:
		{
			return &IndexSelector{
				rangeSelector: value,
				selectorType:  RANGE,
			}
		}
	}
	return nil
}

func (indexSelector *IndexSelector) GetIndex() int {
	return indexSelector.indexSelector
}

func (indexSelector *IndexSelector) GetRange() []int {
	return indexSelector.rangeSelector[:]
}

func (indexSelector *IndexSelector) GetType() IndexType {
	return indexSelector.selectorType
}

func NewPipe(key string, keyType string) *PipeSelector {
	return &PipeSelector{
		keySelector:  key,
		typeSelector: keyType,
	}
}

func (pipeSelector *PipeSelector) GetKey() string {
	return pipeSelector.keySelector
}

func (pipeSelector *PipeSelector) GetType() KeyType {
	switch pipeSelector.typeSelector {
	case "":
		{
			return NONE
		}
	case "string":
		{
			return STRING
		}
	case "number":
		{
			return NUMBER
		}
	default:
		{
			return UNKNOWN
		}
	}
}

func ReadIndex(match string) (int, error) {
	index, err := strconv.Atoi(match)
	if err != nil {
		return 0, err
	}
	if index < 0 {
		return 0, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to read index. invalid index %d", index))
	}
	return index, nil
}

func ReadRange(match string) (*IndexSelector, error) {
	str := strings.Trim(match, string(_WITESPACE))
	str = strings.TrimLeft(str, string(_LPAR))
	str = strings.TrimRight(str, string(_RPAR))
	split := strings.Split(str, string(_COL))
	if len(split) != 2 {
		return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to read range. invalid range %s", match))
	}
	rangeSelector := [2]int{}
	switch split[0] {
	case _BEGIN:
		{
			rangeSelector[0] = -1
		}
	default:
		{
			begin, err := ReadIndex(split[0])
			if err != nil {
				return nil, err
			}
			rangeSelector[0] = begin
		}
	}
	switch split[1] {
	case _END:
		{
			rangeSelector[1] = -1
		}
	default:
		{
			end, err := ReadIndex(split[1])
			if err != nil {
				return nil, err
			}
			rangeSelector[1] = end
		}
	}
	return NewIndex(rangeSelector), nil
}

func ParseArray(match string) (any, error) {
	match = strings.TrimLeft(match, "[")
	match = strings.TrimRight(match, "]")
	keep := strings.HasPrefix(match, _KEEP)
	if keep {
		match = strings.TrimPrefix(match, _KEEP)
	}
	matches := arrayPattern.FindAllString(match, -1)
	slice := make([]*IndexSelector, 0)
	for _, match := range matches {
		char := rune(match[0])
		switch char {
		case _LPAR:
			{
				rng, err := ReadRange(match)
				if err != nil {
					return nil, err
				}
				slice = append(slice, rng)
			}
		default:
			{
				switch match {
				case _EACH:
					{
						slice = append(slice, NewIndex(-1))
					}
				default:
					{
						number, err := ReadIndex(match)
						if err != nil {
							return nil, err
						}
						slice = append(slice, NewIndex(number))
					}
				}
			}
		}
	}
	if keep {
		return KeepDimension(slice), nil
	}
	return slice, nil
}

func ParsePipe(match string) ([]*PipeSelector, error) {
	matches := pipePattern.FindAllString(match, -1)
	slice := make([]*PipeSelector, 0)
	for _, match := range matches {
		split := strings.Split(match, string(_PIPE))
		key := split[0]
		key = strings.TrimLeft(key, string(_SQ))
		key = strings.TrimRight(key, string(_SQ))
		if len(split) == 1 {
			slice = append(slice, NewPipe(key, ""))
			continue
		}
		if len(split) == 2 {
			slice = append(slice, NewPipe(key, split[1]))
			continue
		}
		return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to parse pipe. invalid pipe %s", match))
	}
	return slice, nil
}

func ParseSelector(selector string) ([]any, error) {
	functions := strings.SplitN(selector, "=>", 2)
	slice := make([]any, 0)
	if len(functions) == 2 {
		selector = functions[1]
		slice = append(slice, TopLevelFunctionSelector(functions[0]))
	}
	matches := fullPattern.FindAllString(selector, -1)
	for _, match := range matches {
		char := rune(match[0])
		switch char {
		case _LBRA:
			{
				arraySelector, err := ParseArray(strings.Trim(match, " "))
				if err != nil {
					return nil, err
				}
				slice = append(slice, arraySelector)
			}
		case _LCUR:
			{
				arraySelector, err := ParsePipe(match)
				if err != nil {
					return nil, err
				}
				slice = append(slice, arraySelector)
			}
		default:
			{
				match = strings.TrimLeft(match, string(_SQ))
				match = strings.TrimRight(match, string(_SQ))
				slice = append(slice, KeySelector(match))
			}
		}
	}
	return slice, nil
}

func SelectDimension(data any, dimensions []*IndexSelector) (any, error) {
	if len(dimensions) == 0 {
		return data, nil
	}
	index := dimensions[0]
	switch index.GetType() {
	case RANGE:
		{
			index := index.GetRange()
			begin := index[0]
			if begin == -1 {
				begin = 0
			}
			end := index[1]
			if end == -1 {
				end = len(data.([]any))
			}
			return SelectDimension(data.([]any)[begin:end], dimensions[1:])
		}
	case INDEX:
		{
			index := index.GetIndex()
			if index == -1 {
				slice := make([]any, 0)
				for _, item := range data.([]any) {
					rs, err := SelectDimension(item, dimensions[1:])
					if err != nil {
						return nil, err
					}
					slice = append(slice, rs)
				}
				return slice, nil
			}
			return SelectDimension(data.([]any)[index], dimensions[1:])
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func SelectMany(data []any, dimensions []*IndexSelector) (any, error) {
	rs, err := SelectDimension(data, dimensions)
	if err != nil {
		return nil, err
	}
	if _, ok := rs.([]any); !ok {
		return rs, nil
	}
	return Unwind(rs.([]any), len(dimensions)-1), nil
}

func Unwind(data []any, depth int) []any {
	if depth == 0 {
		return data
	}
	slice := make([]any, 0)
	depth = depth - 1
	for _, item := range data {
		if array, ok := item.([]any); ok {
			slice = append(slice, Unwind(array, depth)...)
			continue
		}
		slice = append(slice, item)
	}
	return slice
}

func SelectObject(data map[string]any, key string) any {
	value, ok := data[key]
	if !ok {
		return nil
	}
	return value
}

var (
	mut   sync.Mutex
	cache map[string][][]any
)

func init() {
	cache = make(map[string][][]any)
}

func ExecReader(data any, selector string) (any, error) {
	mut.Lock()
	if _, ok := cache[selector]; !ok {
		allSelectors := make([][]any, 0)
		selectors := strings.Split(selector, "::")
		for _, item := range selectors {
			selectors, err := ParseSelector(item)
			if err != nil {
				mut.Unlock()
				return nil, err
			}
			allSelectors = append(allSelectors, selectors)
		}
		cache[selector] = allSelectors
	}
	mut.Unlock()
	result := data
	for _, item := range cache[selector] {
		rs, err := ReaderExecutor(result, item)
		if err != nil {
			return nil, err
		}
		result = rs
	}
	return result, nil
}

func ReaderExecutor(data any, selectors []any) (any, error) {
	if len(selectors) == 0 {
		return data, nil
	}
	functionName, ok := selectors[0].(TopLevelFunctionSelector)
	if !ok {
		return Reader(data, selectors)
	}
	rs, err := Reader(data, selectors[1:])
	if err != nil {
		return nil, err
	}
	function, ok := topLevelFunctions[string(functionName)]
	if !ok {
		return nil, INVALID_FUNCTION.Extend(fmt.Sprintf("failed to execute function. %s is not a function", functionName))
	}
	return function(rs)
}

func Reader(data any, selectors []any) (any, error) {
	if len(selectors) == 0 {
		return data, nil
	}
	if data == nil {
		return nil, nil
	}
	selector := selectors[0]
	switch selector := selector.(type) {
	case KeySelector:
		{
			switch data := data.(type) {
			case map[string]any:
				{
					rs := SelectObject(data, string(selector))
					return Reader(rs, selectors[1:])
				}
			case []any:
				{
					slice := make([]any, len(data))
					for index, item := range data {
						rs, err := Reader(item, selectors)
						if err != nil {
							return nil, err
						}
						slice[index] = rs
					}
					return slice, nil

				}
			case func() (any, error):
				{
					rs, err := data()
					if err != nil {
						return nil, err
					}
					return Reader(rs, selectors)
				}
			default:
				{
					return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to execute read operation. key selectors are not valid on %T type", data))
				}
			}

		}
	case []*IndexSelector:
		{
			switch data := data.(type) {
			case []any:
				{
					rs, err := SelectMany(data, selector)
					if err != nil {
						return nil, err
					}
					return Reader(rs, selectors[1:])
				}
			case func() (any, error):
				{
					data, err := data()
					if err != nil {
						return nil, err
					}
					return Reader(data, selectors)
				}
			default:
				{
					return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to execute read operation. index selectors are not valid on %T type", data))
				}
			}

		}
	case KeepDimension:
		{
			switch data := data.(type) {
			case []any:
				{
					rs, err := SelectDimension(data, selector)
					if err != nil {
						return nil, err
					}
					return Reader(rs, selectors[1:])
				}
			case func() (any, error):
				{
					data, err := data()
					if err != nil {
						return nil, err
					}
					return Reader(data, selectors)
				}
			default:
				{
					return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to execute read operation. dimension selectors are not valid on %T type", data))
				}
			}
		}
	case []*PipeSelector:
		{
			switch data := data.(type) {
			case map[string]any:
				{
					copy := make(map[string]any)
					for _, selector := range selector {
						switch selector.GetType() {
						case NONE:
							{
								copy[selector.GetKey()] = data[selector.GetKey()]
							}
						case STRING:
							{
								value := data[selector.GetKey()]
								switch value := value.(type) {
								case float64:
									{
										remainder := math.Mod(value, 1)
										if remainder == 0 {
											copy[selector.GetKey()] = fmt.Sprintf("%d", int64(value))
											continue
										}
										copy[selector.GetKey()] = fmt.Sprintf("%f", value)
									}
								default:
									{
										copy[selector.GetKey()] = fmt.Sprintf("%v", value)
									}
								}

							}
						case NUMBER:
							{
								str, ok := data[selector.GetKey()].(string)
								if !ok {
									return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to execute pipe operation. %s is of %T type", selector.GetKey(), data[selector.GetKey()]))
								}
								number, err := strconv.ParseFloat(str, 64)
								if err != nil {
									return nil, err
								}
								copy[selector.GetKey()] = number
							}
						default:
							{
								return nil, UNSUPPORTED_CASE
							}
						}
					}
					return Reader(copy, selectors[1:])
				}
			case []any:
				{
					slice := make([]any, len(data))
					for index, item := range data {
						rs, err := Reader(item, selectors)
						if err != nil {
							return nil, err
						}
						slice[index] = rs
					}
					return slice, nil
				}
			case func() (any, error):
				{
					data, err := data()
					if err != nil {
						return nil, err
					}
					return Reader(data, selectors)
				}
			default:
				{
					return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to execute read operation. pipe selectors are not valid on %T type", data))
				}
			}

		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func Mix(data any) (any, error) {
	switch data := data.(type) {
	case []any:
		{
			return MixArray(data), nil
		}
	case map[string]any:
		{
			return MixObject(data)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}
func MixArray(data []any) []any {
	slice := make([]any, 0)
	for _, item := range data {
		if array, ok := item.([]any); ok {
			slice = append(slice, MixArray(array)...)
			continue
		}
		slice = append(slice, item)
	}
	return slice
}

func MixObject(data map[string]any) (map[string]any, error) {
	mapper := make(map[string]any)
	for key, item := range data {
		if innerMap, ok := item.(map[string]any); ok {
			rs, err := MixObject(innerMap)
			if err != nil {
				return nil, err
			}
			for innerKey, innerValue := range rs {
				key := fmt.Sprintf("%s_%s", key, innerKey)
				mapper[key] = innerValue
			}
			continue
		}
		mapper[key] = item
	}
	return mapper, nil
}

func Distinct(data any) (any, error) {
	switch data := data.(type) {
	case []any:
		{
			slice := make([]any, 0)
			mapper := make(map[string]bool)
			for _, item := range data {
				sha256 := sha256.New()
				_, err := sha256.Write([]byte(fmt.Sprintf("%v", item)))
				if err != nil {
					return nil, err
				}
				hash := sha256.Sum(nil)
				hashBase64 := base64.StdEncoding.EncodeToString(hash)
				if _, ok := mapper[hashBase64]; !ok {
					mapper[hashBase64] = true
					slice = append(slice, item)
				}
			}
			return slice, nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func RegisterTopLevelFunction(name string, function func(any) (any, error)) {
	if topLevelFunctions == nil {
		topLevelFunctions = make(TopLevelFunction)
	}
	topLevelFunctions[name] = function
}

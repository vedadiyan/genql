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
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/vedadiyan/genql/compare"
	"github.com/vedadiyan/sqlparser/v2"
)

type (
	Map              = map[string]any
	GroupDefinition  = map[string]bool
	WhereDefinition  = *sqlparser.Where
	HavingDefinition = *sqlparser.Where
	SelectDefinition = sqlparser.SelectExprs
	Statement        = sqlparser.Statement
	QueryOption      func(query *Query)
	CteEvaluation    = func() (any, error)
	Function         func(*Query, Map, *FunctionOptions, []any) (any, error)
	NeutalString     string
	ColumnName       string
	Ommit            bool
	Fuse             map[string]any

	FunctionOptions struct {
	}

	ExpressionReaderOptions struct {
	}

	OrderByDefinition []struct {
		Key   string
		Value bool
	}
	Options struct {
		wrapped                 bool
		postgresEscapingDialect bool
		idomaticArrays          bool
		completed               func()
		errors                  func(err error)
		constants               map[string]any
		vars                    map[string]any
		varsMut                 sync.RWMutex
	}
	Query struct {
		data Map
		from []any
		//processed           []any
		distinct            bool
		selectDefinition    SelectDefinition
		whereDefinition     WhereDefinition
		groupDefinition     GroupDefinition
		offsetDefinition    int
		limitDefinition     int
		havingDefinition    HavingDefinition
		orderByDefinition   OrderByDefinition
		wg                  sync.WaitGroup
		singletonExecutions map[string]any
		postProcessors      []func() error
		dual                bool
		options             *Options

		ident string
	}
)

var (
	functions          map[string]Function
	immediateFunctions []string
)

func Wrapped() QueryOption {
	return func(query *Query) {
		query.options.wrapped = true
	}
}

func PostgresEscapingDialect() QueryOption {
	return func(query *Query) {
		query.options.postgresEscapingDialect = true
	}
}

func IdomaticArrays() QueryOption {
	return func(query *Query) {
		query.options.idomaticArrays = true
	}
}

func CompletedCallback(callback func()) QueryOption {
	return func(query *Query) {
		query.options.completed = callback
	}
}

func UnReportedErrors(handler func(error)) QueryOption {
	return func(query *Query) {
		query.options.errors = handler
	}
}

func WithConstants(constants map[string]any) QueryOption {
	return func(query *Query) {
		query.options.constants = constants
	}
}

func WithVars(vars map[string]any) QueryOption {
	return func(query *Query) {
		query.options.vars = vars
	}
}

func New(data Map, query string, options ...QueryOption) (*Query, error) {
	q := &Query{
		offsetDefinition:    -1,
		limitDefinition:     -1,
		groupDefinition:     make(GroupDefinition),
		orderByDefinition:   make(OrderByDefinition, 0),
		singletonExecutions: make(map[string]any),
		postProcessors:      make([]func() error, 0),
		options:             &Options{},
	}
	for _, option := range options {
		option(q)
	}
	switch q.options.wrapped {
	case true:
		{
			q.data = Map{"root": data}
		}
	default:
		{
			q.data = data
		}
	}
	if q.options.postgresEscapingDialect {
		rs, err := DoubleQuotesToBackTick(query)
		if err != nil {
			return nil, err
		}
		query = rs
	}
	if q.options.idomaticArrays {
		rs, err := FixIdiomaticArray(query)
		if err != nil {
			return nil, err
		}
		query = rs
	}
	statement, err := Parse(query)
	if err != nil {
		return nil, err
	}
	err = Build(q, statement)
	if err != nil {
		return nil, err
	}
	return q, nil
}

func Prepare(data Map, statement sqlparser.Statement, options *Options) (*Query, error) {
	q := &Query{
		offsetDefinition:    -1,
		limitDefinition:     -1,
		groupDefinition:     make(GroupDefinition),
		orderByDefinition:   make(OrderByDefinition, 0),
		singletonExecutions: map[string]any{},
		postProcessors:      make([]func() error, 0),
		options:             options,
	}
	q.data = data
	err := Build(q, statement)
	if err != nil {
		return nil, err
	}
	return q, nil
}

func Parse(query string) (Statement, error) {
	return sqlparser.Parse(query)
}

func Build(query *Query, statement Statement) error {
	switch statement := statement.(type) {
	case *sqlparser.Select:
		{
			return BuildSelect(query, statement)
		}
	case *sqlparser.Union:
		{
			return BuildUnion(query, statement)
		}
	default:
		{
			return UNSUPPORTED_CASE.Extend(fmt.Sprintf("%T is not supported", statement))
		}
	}
}

func BuildSelect(query *Query, slct *sqlparser.Select) error {
	if len(slct.From) > 1 {
		return EXPECTATION_FAILED.Extend("this version of gql does not support multiple table selection")
	}
	err := BuildCte(query, slct.With)
	if err != nil {
		return err
	}
	err = BuildFrom(query, &slct.From[0])
	if err != nil {
		return err
	}
	err = BuildLimit(query, slct.Limit)
	if err != nil {
		return err
	}
	err = BuildGroup(query, slct.GroupBy)
	if err != nil {
		return err
	}
	err = BuildOrder(query, &slct.OrderBy)
	if err != nil {
		return err
	}
	query.havingDefinition = slct.Having
	query.selectDefinition = *slct.SelectExprs
	query.whereDefinition = slct.Where
	query.distinct = slct.Distinct
	return nil
}

func BuildUnion(query *Query, expr *sqlparser.Union) error {
	leftStatement := expr.Left.(*sqlparser.Select)
	leftStatement.With = expr.With
	rightStatement := expr.Right.(*sqlparser.Select)
	rightStatement.With = expr.With
	left, err := Prepare(query.data, leftStatement, query.options)
	if err != nil {
		return err
	}
	leftData, err := left.execAndPostProcess()
	if err != nil {
		return err
	}
	right, err := Prepare(query.data, rightStatement, query.options)
	if err != nil {
		return err
	}
	rightData, err := right.execAndPostProcess()
	if err != nil {
		return err
	}
	leftDataArray, err := AsArray(leftData)
	if err != nil {
		return err
	}
	rightDataArray, err := AsArray(rightData)
	if err != nil {
		return err
	}

	slice := make([]any, 0)
	slice = append(slice, leftDataArray...)
	slice = append(slice, rightDataArray...)
	query.from = slice
	query.selectDefinition = sqlparser.SelectExprs{}
	query.selectDefinition.Exprs = make([]sqlparser.SelectExpr, 0)
	err = BuildLimit(query, expr.Limit)
	if err != nil {
		return err
	}
	return nil
}

func BuildCte(query *Query, expr *sqlparser.With) error {
	if expr == nil {
		return nil
	}
	for _, cte := range expr.CTEs {
		copy := *cte
		query.data[copy.ID.String()] = CteEvaluation(func() (any, error) {
			query, err := Prepare(query.data, copy.Subquery, query.options)
			if err != nil {
				return nil, err
			}
			rs, err := query.execAndPostProcess()
			if err != nil {
				return nil, err
			}
			query.data[copy.ID.String()] = rs
			return rs, nil
		})
	}
	return nil
}

func BuildLimit(query *Query, limit *sqlparser.Limit) error {
	if limit == nil {
		return nil
	}
	if limit.Offset != nil {
		_, offsetLiteral, err := BuildLiteral(limit.Offset)
		if err != nil {
			return err
		}
		offsetNumeric, err := strconv.Atoi(offsetLiteral)
		if err != nil {
			return err
		}
		query.offsetDefinition = offsetNumeric
	}
	_, limitLiteral, err := BuildLiteral(limit.Rowcount)
	if err != nil {
		return err
	}
	limitNumeric, err := strconv.Atoi(limitLiteral)
	if err != nil {
		return err
	}
	query.limitDefinition = limitNumeric
	return nil
}

func BuildGroup(query *Query, group *sqlparser.GroupBy) error {
	if group == nil {
		return nil
	}
	for _, i := range group.Exprs {
		qualifier, name, err := BuildColumnName(i)
		if err != nil {
			return nil
		}
		if len(qualifier) == 0 {
			query.groupDefinition[name] = true
			continue
		}
		query.groupDefinition[fmt.Sprintf("%s.%s", qualifier, name)] = true
	}
	return nil
}

func BuildOrder(query *Query, orderBy *sqlparser.OrderBy) error {
	if orderBy == nil {
		return nil
	}
	for _, ordeorderBy := range *orderBy {
		qualifier, columnName, err := BuildColumnName(ordeorderBy.Expr)
		if err != nil {
			return err
		}
		if len(qualifier) == 0 {
			query.orderByDefinition = append(query.orderByDefinition, struct {
				Key   string
				Value bool
			}{
				Key:   columnName,
				Value: ordeorderBy.Direction == sqlparser.AscOrder,
			})
			continue
		}
		query.orderByDefinition = append(query.orderByDefinition, struct {
			Key   string
			Value bool
		}{
			Key:   fmt.Sprintf("%s.%s", qualifier, columnName),
			Value: ordeorderBy.Direction == sqlparser.AscOrder,
		})
	}
	return nil
}

func BuildFrom(query *Query, tableExpr *sqlparser.TableExpr) error {
	switch tableExpr := (*tableExpr).(type) {
	case *sqlparser.AliasedTableExpr:
		{
			return BuildFromAliasedTable(query, tableExpr.As.String(), tableExpr.Expr)
		}
	case *sqlparser.JoinTableExpr:
		{
			return BuildJoin(query, tableExpr)
		}
	default:
		{
			return EXPECTATION_FAILED.Extend("invalid from clause")
		}
	}
}

func BuildJoin(query *Query, joinExpr *sqlparser.JoinTableExpr) error {
	left := CopyQuery(query)
	err := BuildFrom(left, &joinExpr.LeftExpr)
	if err != nil {
		return err
	}
	right := CopyQuery(query)
	err = BuildFrom(right, &joinExpr.RightExpr)
	if err != nil {
		return err
	}
	rs, err := ExecJoin(query, left.from, right.from, left.ident, right.ident, joinExpr.Into, joinExpr.Condition.On, joinExpr.Join)
	if err != nil {
		return nil
	}
	query.from = rs
	return nil
}

func ExecJoin(query *Query, left []any, right []any, leftIdent string, rightIdent string, into string, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
	return ExecJoin3(query, left, right, leftIdent, rightIdent, into, joinExpr, joinType)
}

func BuildLiteral(expr sqlparser.Expr) (sqlparser.ValType, string, error) {
	literal, ok := expr.(*sqlparser.Literal)
	if !ok {
		return 0, "", INVALID_TYPE.Extend(fmt.Sprintf("failed to build `LITERAL` expression, expected Literal but found %T", expr))
	}
	return literal.Type, literal.Val, nil
}

func BuildColumnName(expr sqlparser.Expr) (string, string, error) {
	columnName, ok := expr.(*sqlparser.ColName)
	if !ok {
		return "", "", INVALID_TYPE.Extend(fmt.Sprintf("failed to build `COLUMN` name. expected ColName but found %T", expr))
	}
	return columnName.Qualifier.Name.String(), columnName.Name.String(), nil
}

func BuildFromAliasedTable(query *Query, as string, expr sqlparser.SimpleTableExpr) error {
	switch expr := expr.(type) {
	case sqlparser.TableName:
		{
			var tableName string
			qualifier := expr.Qualifier.String()
			name := expr.Name.String()
			if len(qualifier) == 0 {
				tableName = name
			} else {
				tableName = fmt.Sprintf("%s.%s", qualifier, name)
			}
			if len(as) == 0 {
				query.ident = strings.SplitN(tableName, ".", 2)[0]
			} else {
				query.ident = as
			}
			data, err := ExecReader(query.data, tableName)
			if err != nil {
				return err
			}
			switch data := data.(type) {
			case CteEvaluation:
				{
					data, err := data()
					if err != nil {
						return err
					}
					array, err := AsArray(data)
					if err != nil {
						return err
					}
					alias := ProcessAlias(array, as)
					query.from = alias
					return nil
				}
			case nil:
				{
					if qualifier == "" && tableName == "dual" {
						query.dual = true
						array, err := AsArray(query.data)
						if err != nil {
							return err
						}
						query.from = array
						return nil
					}
					return nil
				}
			default:
				{
					array, err := AsArray(data)
					if err != nil {
						return err
					}
					alias := ProcessAlias(array, as)
					query.from = alias
					return nil
				}
			}
		}
	case *sqlparser.DerivedTable:
		{
			subquery, err := Prepare(query.data, expr.Select, query.options)
			if err != nil {
				return err
			}
			data, err := subquery.exec()
			if err != nil {
				return err
			}
			query.postProcessors = append(query.postProcessors, subquery.postProcessors...)
			query.wg.Add(1)
			go func() {
				subquery.wg.Wait()
				query.wg.Done()
			}()
			array, err := AsArray(data)
			if err != nil {
				return err
			}
			alias := ProcessAlias(array, as)
			query.from = alias
			return nil
		}
	default:
		{
			return UNSUPPORTED_CASE.Extend("invalid from clause")
		}
	}
}

func ProcessAlias(data []any, as string) []any {
	if len(as) == 0 {
		return data
	}
	slice := make([]any, len(data))
	for i, j := range data {
		slice[i] = Map{
			as: j,
		}
	}
	return slice
}

func Expr(query *Query, current Map, expr sqlparser.Expr, options *ExpressionReaderOptions) (any, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		{
			return AndExpr(query, current, expr)
		}
	case *sqlparser.OrExpr:
		{
			return OrExpr(query, current, expr)
		}
	case *sqlparser.ComparisonExpr:
		{
			return ComparisonExpr(query, current, expr)
		}
	case *sqlparser.BetweenExpr:
		{
			return BetweenExpr(query, current, expr)
		}
	case *sqlparser.BinaryExpr:
		{
			return BinaryExpr(query, current, expr)
		}
	case *sqlparser.Literal:
		{
			return LiteralExpr(query, current, expr)
		}
	case *sqlparser.NullVal:
		{
			return nil, nil
		}
	case *sqlparser.IsExpr:
		{
			return IsExpr(query, current, expr)
		}
	case *sqlparser.NotExpr:
		{
			return NotExpr(query, current, expr)
		}
	case *sqlparser.SubstrExpr:
		{
			return SubStrExpr(query, current, expr)
		}
	case *sqlparser.UnaryExpr:
		{
			return UnaryExpr(query, current, expr)
		}
	case sqlparser.ValTuple:
		{
			return ValueTupleExpr(query, current, &expr)
		}
	case sqlparser.BoolVal:
		{
			return bool(expr), nil
		}
	case *sqlparser.ColName:
		{
			qualifier, name, err := BuildColumnName(expr)
			if err != nil {
				return nil, err
			}
			columnName := name
			if len(qualifier) > 0 {
				columnName = fmt.Sprintf("%s.%s", qualifier, name)
			}
			return ColumnName(columnName), nil
		}
	case *sqlparser.Subquery:
		{
			return SubqueryExpr(query, current, expr)
		}
	case *sqlparser.CaseExpr:
		{
			return CaseExpr(query, current, expr)
		}
	case *sqlparser.ExistsExpr:
		{
			return ExistExpr(query, current, expr)
		}
	case *sqlparser.FuncExpr:
		{
			return FunExpr(query, current, expr)
		}
	case sqlparser.AggrFunc:
		{
			return AggrFunExpr(query, current, expr)
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func AndExpr(query *Query, current Map, expr *sqlparser.AndExpr) (bool, error) {
	left, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return false, err
	}
	leftValueRaw, err := ValueOf(query, current, left)
	if err != nil {
		return false, err
	}
	if leftValueRaw == nil {
		return false, EXPECTATION_FAILED.Extend("failed to build `AND` expreesion. left side value is nil")
	}
	leftValue, err := AsType[bool](leftValueRaw)
	if err != nil {
		return false, err
	}
	right, err := Expr(query, current, expr.Right, nil)
	if err != nil {
		return false, err
	}
	rightValueRaw, err := ValueOf(query, current, right)
	if err != nil {
		return false, err
	}
	if rightValueRaw == nil {
		return false, EXPECTATION_FAILED.Extend("failed to build `AND` expreesion. right side value is nil")
	}
	rightValue, err := AsType[bool](rightValueRaw)
	if err != nil {
		return false, err
	}
	return *leftValue && *rightValue, nil
}

func OrExpr(query *Query, current Map, expr *sqlparser.OrExpr) (bool, error) {
	left, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return false, err
	}
	leftValueRaw, err := ValueOf(query, current, left)
	if err != nil {
		return false, err
	}
	if leftValueRaw == nil {
		return false, EXPECTATION_FAILED.Extend("failed to build `OR` expreesion. left side value is nil")
	}
	leftValue, err := AsType[bool](leftValueRaw)
	if err != nil {
		return false, err
	}
	right, err := Expr(query, current, expr.Right, nil)
	if err != nil {
		return false, err
	}
	rightValueRaw, err := ValueOf(query, current, right)
	if err != nil {
		return false, err
	}
	if rightValueRaw == nil {
		return false, EXPECTATION_FAILED.Extend("failed to build `OR` expreesion. right side value is nil")
	}
	rightValue, err := AsType[bool](rightValueRaw)
	if err != nil {
		return false, err
	}
	return *leftValue || *rightValue, nil
}

func ComparisonExpr(query *Query, current Map, expr *sqlparser.ComparisonExpr) (bool, error) {
	current["<-"] = query.data
	defer delete(current, "<-")
	left, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return false, err
	}
	leftValue, err := ValueOf(query, current, left)
	if err != nil {
		return false, err
	}
	right, err := Expr(query, current, expr.Right, nil)
	if err != nil {
		return false, err
	}
	rightValue, err := ValueOf(query, current, right)
	if err != nil {
		return false, err
	}

	switch expr.Operator {
	case sqlparser.EqualOp:
		{
			return compare.Compare(leftValue, rightValue) == 0, nil
		}
	case sqlparser.NotEqualOp:
		{
			return compare.Compare(leftValue, rightValue) != 0, nil
		}
	case sqlparser.GreaterThanOp:
		{
			return compare.Compare(leftValue, rightValue) == 1, nil
		}
	case sqlparser.GreaterEqualOp:
		{
			return compare.Compare(leftValue, rightValue) >= 0, nil
		}
	case sqlparser.LessThanOp:
		{
			return compare.Compare(leftValue, rightValue) == -1, nil
		}
	case sqlparser.LessEqualOp:
		{
			return compare.Compare(leftValue, rightValue) <= 0, nil
		}
	case sqlparser.LikeOp:
		{
			return RegexComparison(fmt.Sprintf("%v", leftValue), fmt.Sprintf("%v", rightValue))
		}
	case sqlparser.NotLikeOp:
		{
			rs, err := RegexComparison(fmt.Sprintf("%v", leftValue), fmt.Sprintf("%v", rightValue))
			if err != nil {
				return false, err
			}
			return !rs, nil
		}
	case sqlparser.InOp:
		{
			if right == nil {
				return false, EXPECTATION_FAILED.Extend("failed to build `IN` expreesion. right side value is nil")
			}
			rightArray, ok := (right).([]any)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `IN` expression. expected an array but found %T", right))
			}
			for _, value := range rightArray {
				switch value := value.(type) {
				case Map:
					{
						for _, value := range value {
							if v, ok := value.(*float64); ok {
								value = *v
							}
							if compare.Compare(leftValue, value) == 0 {
								return true, nil
							}
							break
						}
					}
				default:
					{
						if v, ok := value.(*float64); ok {
							value = *v
						}
						if compare.Compare(leftValue, value) == 0 {
							return true, nil
						}
					}
				}
			}
			return false, nil
		}
	case sqlparser.NotInOp:
		{
			if right == nil {
				return false, EXPECTATION_FAILED.Extend("failed to build `NOT IN` expreesion. right side value is nil")
			}
			rightArray, ok := (right).([]any)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `IN` expression. expected an array but found %T", right))
			}
			for _, value := range rightArray {
				if leftValue == fmt.Sprintf("%v", value) {
					return false, nil
				}
			}
			return true, nil
		}
	default:
		{
			return false, UNSUPPORTED_CASE
		}
	}
}

func BetweenExpr(query *Query, current Map, expr *sqlparser.BetweenExpr) (bool, error) {
	point, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return false, err
	}
	pointValueRaw, err := ValueOf(query, current, point)
	if err != nil {
		return false, err
	}
	// TO DO: could be either a number or a date
	from, err := Expr(query, current, expr.From, nil)
	if err != nil {
		return false, err
	}
	// TO DO: could be either a number or a date
	to, err := Expr(query, current, expr.To, nil)
	if err != nil {
		return false, err
	}
	pointValue := fmt.Sprintf("%v", pointValueRaw)
	fromValue := fmt.Sprintf("%v", from)
	toValue := fmt.Sprintf("%v", to)
	switch expr.IsBetween {
	case true:
		{
			return (pointValue > fromValue) && (pointValue < toValue), nil
		}
	default:
		{
			return !((pointValue > fromValue) && (pointValue < toValue)), nil
		}
	}
}

func BinaryExpr(query *Query, current Map, expr *sqlparser.BinaryExpr) (*float64, error) {
	left, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return nil, err
	}
	leftValueRaw, err := ValueOf(query, current, left)
	if err != nil {
		return nil, err
	}
	if leftValueRaw == nil {
		return nil, nil
	}
	leftValue, err := AsType[float64](leftValueRaw)
	if err != nil {
		return nil, err
	}
	right, err := Expr(query, current, expr.Right, nil)
	if err != nil {
		return nil, err
	}
	rightValueRaw, err := ValueOf(query, current, right)
	if err != nil {
		return nil, err
	}
	if rightValueRaw == nil {
		return nil, nil
	}
	rightValue, err := AsType[float64](rightValueRaw)
	if err != nil {
		return nil, err
	}
	switch expr.Operator {
	case sqlparser.PlusOp:
		{
			rs := *leftValue + *rightValue
			return &rs, nil
		}
	case sqlparser.MinusOp:
		{
			rs := *leftValue - *rightValue
			return &rs, nil
		}
	case sqlparser.MultOp:
		{
			rs := *leftValue * *rightValue
			return &rs, nil
		}
	case sqlparser.DivOp:
		{
			rs := *leftValue / *rightValue
			return &rs, nil
		}
	case sqlparser.IntDivOp:
		{
			rs := float64(int64(*leftValue) / int64(*rightValue))
			return &rs, nil
		}
	case sqlparser.ModOp:
		{
			rs := math.Mod(*leftValue, *rightValue)
			return &rs, nil
		}
	case sqlparser.BitAndOp:
		{
			rs := float64(int64(*leftValue) & int64(*rightValue))
			return &rs, nil
		}
	case sqlparser.BitOrOp:
		{
			rs := float64(int64(*leftValue) | int64(*rightValue))
			return &rs, nil
		}
	case sqlparser.BitXorOp:
		{
			rs := float64(int64(*leftValue) ^ int64(*rightValue))
			return &rs, nil
		}
	case sqlparser.ShiftLeftOp:
		{
			rs := float64(int64(*leftValue) << int64(*rightValue))
			return &rs, nil
		}
	case sqlparser.ShiftRightOp:
		{
			rs := float64(int64(*leftValue) >> int64(*rightValue))
			return &rs, nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func LiteralExpr(query *Query, current Map, expr *sqlparser.Literal) (any, error) {
	literalType, literalValue, err := BuildLiteral(expr)
	if err != nil {
		return nil, err
	}
	switch literalType {
	case sqlparser.DecimalVal, sqlparser.FloatVal, sqlparser.IntVal:
		{
			n, err := strconv.ParseFloat(literalValue, 64)
			if err != nil {
				return nil, err
			}
			return n, nil
		}
	case sqlparser.StrVal:
		{
			return NeutalString(literalValue), nil
		}
	default:
		{
			return nil, UNSUPPORTED_CASE
		}
	}
}

func IsExpr(query *Query, current Map, expr *sqlparser.IsExpr) (bool, error) {
	left, err := Expr(query, current, expr.Left, nil)
	if err != nil {
		return false, err
	}
	leftValue, err := ValueOf(query, current, left)
	if err != nil {
		return false, err
	}
	switch expr.Right {
	case sqlparser.IsNullOp:
		{
			return leftValue == nil, nil
		}
	case sqlparser.IsNotNullOp:
		{
			return leftValue != nil, nil
		}
	case sqlparser.IsTrueOp, sqlparser.IsNotFalseOp:
		{
			if leftValue == nil {
				return false, EXPECTATION_FAILED.Extend("failed to build `IS` expreesion. left side value is nil")
			}
			leftValue, ok := (leftValue).(bool)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `IN` expression. expected a boolean but found %T", left))
			}
			return leftValue, nil
		}
	case sqlparser.IsNotTrueOp, sqlparser.IsFalseOp:
		{
			if leftValue == nil {
				return false, EXPECTATION_FAILED.Extend("failed to build `IS` expreesion. left side value is nil")
			}
			leftValue, ok := (leftValue).(bool)
			if !ok {
				return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `IN` expression. expected a boolean but found %T", left))
			}
			return !leftValue, nil
		}
	default:
		{
			return false, UNSUPPORTED_CASE
		}
	}
}

func NotExpr(query *Query, current Map, expr *sqlparser.NotExpr) (bool, error) {
	rs, err := Expr(query, current, expr.Expr, nil)
	if err != nil {
		return false, err
	}
	rsValueRaw, err := ValueOf(query, current, rs)
	if err != nil {
		return false, err
	}
	if rsValueRaw == nil {
		return false, EXPECTATION_FAILED.Extend("failed to build `NOT` expreesion. left side value is nil")
	}
	rsValue, err := AsType[bool](rsValueRaw)
	if err != nil {
		return false, err
	}
	return !*rsValue, nil
}

func SubStrExpr(query *Query, current Map, expr *sqlparser.SubstrExpr) (string, error) {
	str, err := Expr(query, current, expr.Name, nil)
	if err != nil {
		return "", err
	}
	strValueRaw, err := ValueOf(query, current, str)
	if err != nil {
		return "", err
	}
	if strValueRaw == nil {
		return "", EXPECTATION_FAILED.Extend("failed to build `SubStr` expreesion. the given value is nil")
	}
	strValue, err := AsType[string](strValueRaw)
	if err != nil {
		return "", err
	}
	from, err := Expr(query, current, expr.From, nil)
	if err != nil {
		return "", err
	}
	if from == nil {
		return "", EXPECTATION_FAILED.Extend("failed to build `IS` expreesion. the `from` argument is nil")
	}
	if colName, ok := from.(ColumnName); ok {
		from, err = ExecReader(current, string(colName))
		if err != nil {
			return "", err
		}
	}
	fromValue, err := AsType[float64](from)
	if err != nil {
		return "", err
	}
	to, err := Expr(query, current, expr.To, nil)
	if err != nil {
		return "", err
	}
	if to == nil {
		return "", EXPECTATION_FAILED.Extend("failed to build `IS` expreesion. the `to` argument is nil")
	}
	if colName, ok := to.(ColumnName); ok {
		to, err = ExecReader(current, string(colName))
		if err != nil {
			return "", err
		}
	}
	toValue, err := AsType[float64](to)
	if err != nil {
		return "", err
	}
	return string((*strValue)[int(*fromValue):int(*fromValue+*toValue)]), nil
}

func UnaryExpr(query *Query, current Map, expr *sqlparser.UnaryExpr) (any, error) {
	val, err := Expr(query, current, expr.Expr, nil)
	if err != nil {
		return nil, err
	}
	valRawValue, err := ValueOf(query, current, val)
	if err != nil {
		return nil, err
	}
	if valRawValue == nil {
		return nil, EXPECTATION_FAILED.Extend("failed to build `UNARY` expreesion. the given value is nil")
	}
	switch expr.Operator {
	case sqlparser.TildaOp:
		{
			valValue, err := AsType[float64](valRawValue)
			if err != nil {
				return nil, err
			}
			rs := float64(^int64(*valValue))
			return &rs, nil
		}
	case sqlparser.UMinusOp:
		{
			valValue, err := AsType[float64](valRawValue)
			if err != nil {
				return nil, err
			}
			rs := -1 * *valValue
			return &rs, nil
		}
	case sqlparser.BangOp:
		{
			valValue, err := AsType[bool](valRawValue)
			if err != nil {
				return nil, err
			}
			rs := !(*valValue)
			return rs, nil
		}
	default:
		{
			return nil, UNDEFINED_OPERATOR
		}
	}
}

func ValueTupleExpr(query *Query, current Map, expr *sqlparser.ValTuple) ([]any, error) {
	if expr == nil {
		return nil, EXPECTATION_FAILED.Extend("failed to build `VALUE TUPLE` expreesion. the expression is nil")
	}
	slice := make([]any, 0)
	for _, value := range *expr {
		value, err := Expr(query, current, value, nil)
		if err != nil {
			return nil, err
		}
		if colName, ok := value.(ColumnName); ok {
			value, err = ExecReader(current, string(colName))
			if err != nil {
				return nil, err
			}
		}
		slice = append(slice, value)
	}
	return slice, nil
}

func SelectExpr(query *Query, current Map, expr *sqlparser.SelectExprs) (Map, error) {
	data := make(Map)
	for _, expr := range expr.Exprs {
		switch expr := expr.(type) {
		case *sqlparser.StarExpr:
			{
				for key, value := range current {
					query.postProcessors = append(query.postProcessors, func() error {
						delete(data, "<-")
						return nil
					})
					data[key] = value
				}
			}
		case *sqlparser.AliasedExpr:
			{
				value, err := Expr(query, current, expr.Expr, nil)
				if err != nil {
					return nil, err
				}
				if _, ok := value.(Ommit); ok {
					continue
				}
				valueRaw, err := ValueOf(query, current, value)
				if err != nil {
					return nil, err
				}
				// Fuse blends all the keys to the current row
				// Fuse types only come from the FuseFunc function
				if fuse, ok := valueRaw.(Fuse); ok {
					prefix := expr.As.String()
					for key, value := range fuse {
						if len(prefix) > 0 {
							data[fmt.Sprintf("%s.%s", prefix, key)] = value
							continue
						}
						data[key] = value
					}
					continue
				}
				name := expr.ColumnName()
				if len(expr.As.String()) > 0 {
					name = expr.As.String()
				}
				// Async functions return pointers
				// It's a good idea to convert them back to value types
				if valueRaw, ok := valueRaw.(*any); ok {
					query.postProcessors = append(query.postProcessors, func() error {
						if err != nil {
							return err
						}

						value := *valueRaw
						for {
							x, ok := value.(*any)
							if !ok {
								break
							}
							value = *x
						}

						data[name] = value
						return nil
					})
				}
				data[name] = valueRaw
			}
		}
	}
	return data, nil
}

func SubqueryExpr(query *Query, current Map, expr *sqlparser.Subquery) (any, error) {
	// Backward Navigation
	current["<-"] = query.data
	query.postProcessors = append(query.postProcessors, func() error {
		delete(current, "<-")
		return nil
	})
	subQuery, err := Prepare(current, expr.Select, query.options)
	if err != nil {
		return nil, err
	}
	rs, err := subQuery.exec()
	if err != nil {
		return nil, err
	}
	query.postProcessors = append(query.postProcessors, subQuery.postProcessors...)
	query.wg.Add(1)
	go func() {
		subQuery.wg.Wait()
		query.wg.Done()
	}()
	return rs, nil
}

func CaseExpr(query *Query, current Map, expr *sqlparser.CaseExpr) (any, error) {
	for _, when := range expr.Whens {
		rs, err := Expr(query, current, when.Cond, nil)
		if err != nil {
			return nil, err
		}
		value, ok := rs.(bool)
		if !ok {
			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `CASE` caluse. expected a boolean but found %T", value))
		}
		if value {
			return Expr(query, current, when.Val, nil)
		}
	}
	if expr.Else == nil {
		return Expr(query, current, &sqlparser.NullVal{}, nil)
	}
	return Expr(query, current, expr.Else, nil)
}

// KNOWN ISSUE:
// The existing Exist function is inefficient as it does not break when
// it finds the first value
func ExistExpr(query *Query, current Map, expr *sqlparser.ExistsExpr) (bool, error) {
	// Backward Navigation
	current["<-"] = query.data
	query.postProcessors = append(query.postProcessors, func() error {
		delete(current, "<-")
		return nil
	})
	q, err := Prepare(current, expr.Subquery.Select, query.options)
	if err != nil {
		return false, err
	}
	for i := 0; i < len(q.from); i++ {
		item, ok := q.from[i].(Map)
		if !ok {
			return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `EXIST` expression. expected an object but found %T", item))
		}
		for key, value := range current {
			item[key] = value
		}
		q.from[i] = item
	}
	rs, err := q.exec()
	array, ok := rs.([]any)
	if !ok {
		return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `EXIST` expression. expected an array but found %T", array))
	}
	query.postProcessors = append(query.postProcessors, q.postProcessors...)
	query.wg.Add(1)
	go func() {
		q.wg.Wait()
		query.wg.Done()
	}()
	return len(array) > 0, err
}

func FunExpr(query *Query, current Map, expr *sqlparser.FuncExpr) (any, error) {
	name := expr.Name.Lowered()

	if name == "await" {
		var rs any
		var err error
		query.postProcessors = append(query.postProcessors, func() error {
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				err = e
				return e
			}
			rs = slice[0]
			return nil
		})
		return &rs, err
	}

	function, ok := functions[expr.Name.Lowered()]
	if !ok {
		return nil, INVALID_FUNCTION.Extend(fmt.Sprintf("function %s cannot be found", expr.Name.String()))
	}
	execType := strings.ToLower(expr.Qualifier.String())
	isimmediate := IsImmediateFunction(name)
	switch execType {
	case "async":
		{
			if isimmediate {
				return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("%s is an immediate function and it cannot be run asynchronously", name))
			}
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				return nil, e
			}
			var rs any
			var err error
			query.wg.Add(1)
			go func() {
				rs, err = function(query, current, nil, slice)
				query.wg.Done()
			}()
			return &rs, err
		}
	case "spin":
		{
			if isimmediate {
				return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("%s is an immediate function and it cannot be spinned", name))
			}
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				return nil, e
			}
			go func() {
				_, err := function(query, current, nil, slice)
				if err != nil {
					if query.options.errors != nil {
						query.options.errors(err)
					}
				}
			}()
			return Ommit(true), nil
		}
	case "spinasync":
		{
			if isimmediate {
				return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("%s is an immediate function and it cannot be spinned asynchronously", name))
			}
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				return nil, e
			}
			query.wg.Add(1)
			go func() {
				_, err := function(query, current, nil, slice)
				if err != nil {
					if query.options.errors != nil {
						query.options.errors(err)
					}
				}
				query.wg.Done()
			}()
			return Ommit(true), nil
		}
	case "once":
		{
			name := fmt.Sprintf("%s.%s", strings.ToLower(expr.Qualifier.String()), expr.Name.Lowered())
			rs, ok := query.singletonExecutions[name]
			if !ok {
				slice, e := FuncArgReader(query, current, expr.Exprs)
				if e != nil {
					return nil, e
				}
				rs, err := function(query, current, nil, slice)
				if err != nil {
					return nil, err
				}
				query.singletonExecutions[name] = rs
				return rs, nil
			}
			return rs, nil
		}
	case "global":
		{
			name := fmt.Sprintf("%s.%s", strings.ToLower(expr.Qualifier.String()), expr.Name.Lowered())
			rs, ok := query.singletonExecutions[name]
			if !ok {
				exprs := make([]sqlparser.Expr, 0)
				for _, expr := range expr.Exprs {
					aliasedExpr, ok := expr.(*sqlparser.Subquery)
					if !ok {
						return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to build global `FUNCTION`. expected aliased expression but found %T", expr))
					}
					exprs = append(exprs, aliasedExpr)
				}
				slice, err := AggrFuncArgReader(query, current, sqlparser.Exprs{Exprs: exprs})
				if err != nil {
					return nil, err
				}
				rs, err := function(query, current, nil, slice)
				if err != nil {
					return nil, err
				}
				query.singletonExecutions[name] = rs
				return rs, nil
			}
			return rs, nil
		}
	case "scoped":
		{
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				return nil, e
			}
			return function(query, current, nil, slice)
		}
	default:
		{
			slice, e := FuncArgReader(query, current, expr.Exprs)
			if e != nil {
				return nil, e
			}
			return function(query, current, nil, slice)
		}
	}
}
func AggrFunExpr(query *Query, current Map, expr sqlparser.AggrFunc) (any, error) {
	name := strings.ToLower(expr.AggrName())
	function, ok := functions[name]
	if !ok {
		return nil, INVALID_FUNCTION.Extend(fmt.Sprintf("function %s cannot be found", expr.AggrName()))
	}
	if len(query.groupDefinition) != 0 {
		slice, err := AggrFuncArgReader(query, current, sqlparser.Exprs{Exprs: expr.GetArgs()})
		if err != nil {
			return nil, err
		}
		result, err := function(query, current, nil, slice)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	rs, ok := query.singletonExecutions[name]
	if !ok {
		slice, err := AggrFuncArgReader(query, map[string]any{"*": query.from}, sqlparser.Exprs{Exprs: expr.GetArgs()})
		if err != nil {
			return nil, err
		}
		result, err := function(query, current, nil, slice)
		if err != nil {
			return nil, err
		}
		query.singletonExecutions[name] = result
		return result, nil
	}
	return rs, nil
}

func FuncArgReader(query *Query, current Map, selectExprs []sqlparser.Expr) ([]any, error) {
	exprs := make([]sqlparser.Expr, 0)
	for _, expr := range selectExprs {
		aliasedExpr, ok := expr.(*sqlparser.Subquery)
		if !ok {
			return nil, EXPECTATION_FAILED.Extend(fmt.Sprintf("failed to build `FUNCTION ARGUMENT`. expected aliased expression but found %T", expr))
		}
		exprs = append(exprs, aliasedExpr)
	}
	slice := make([]any, 0)
	for _, expr := range exprs {
		rs, err := Expr(query, current, expr, nil)
		if err != nil {
			return nil, err
		}
		value, err := ValueOf(query, current, rs)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}
	return slice, nil
}

func AggrFuncArgReader(query *Query, current Map, exprs sqlparser.Exprs) ([]any, error) {
	slice := make([]any, 0)
	for _, expr := range exprs.Exprs {
		rs, err := Expr(query, current, expr, nil)
		if err != nil {
			return nil, err
		}
		if columnName, ok := rs.(ColumnName); ok {
			data := any(current)
			all, ok := current["*"]
			if ok {
				slice, ok := all.([]any)
				if ok {
					data = slice
				}
			}
			rs, err := ExecReader(data, string(columnName))
			if err != nil {
				return nil, err
			}
			slice = append(slice, rs)
			continue
		}
		value, err := ValueOf(query, current, rs)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}
	return slice, nil
}

func ExecWhere(query *Query, current Map) (bool, error) {
	if query.whereDefinition != nil {
		rs, err := Expr(query, current, query.whereDefinition.Expr, nil)
		if err != nil {
			return false, err
		}
		result, ok := rs.(bool)
		if !ok {
			return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `WHERE` expression. expected a boolean but found %T", result))
		}
		return result, nil
	}
	return true, nil
}

func ExecGroupBy(query *Query, current []any) ([]any, error) {
	if len(query.groupDefinition) == 0 {
		return current, nil
	}
	grouped := make(map[*map[string]any][]any)
	for _, item := range current {
		innerMap := make(map[string]any)
		for key := range query.groupDefinition {
			rs, err := ExecReader(item, key)
			if err != nil {
				return nil, err
			}
			innerMap[key] = rs
		}
		var ref *map[string]any
		for group := range grouped {
			isMatch := true
			for key, value := range innerMap {
				if (*group)[key] != value {
					isMatch = false
					break
				}
			}
			if isMatch {
				ref = group
				break
			}
		}
		if ref != nil {
			grouped[ref] = append(grouped[ref], item)
			continue
		}
		grouped[&innerMap] = make([]any, 0)
		grouped[&innerMap] = append(grouped[&innerMap], item)
	}
	slice := make([]any, 0)
	for key, item := range grouped {
		current := make(Map)
		for innerKey, innerValue := range *key {
			current[innerKey] = innerValue
		}
		current["*"] = item
		rs, err := ExecHaving(query, current)
		if err != nil {
			return nil, err
		}
		if rs {
			slice = append(slice, current)
		}

	}
	return slice, nil
}

func ExecHaving(query *Query, current Map) (bool, error) {
	if query.havingDefinition != nil {
		rs, err := Expr(query, current, query.havingDefinition.Expr, nil)
		if err != nil {
			return false, err
		}
		result, ok := rs.(bool)
		if !ok {
			return false, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `HAVING` expression. expected a boolean but found %T", result))
		}
		return result, nil
	}
	return true, nil
}

func IsSelectAllAggregate(query *Query) bool {
	for _, slct := range query.selectDefinition.Exprs {
		expr, ok := slct.(*sqlparser.AliasedExpr)
		if !ok {
			return false
		}
		if _, ok := expr.Expr.(sqlparser.AggrFunc); !ok {
			return false
		}
	}
	return true
}

func ExecSelect(query *Query, current []any) ([]any, error) {
	copy := make([]any, 0)
	if IsSelectAllAggregate(query) {
		rs, err := SelectExpr(query, nil, &query.selectDefinition)
		if err != nil {
			return nil, err
		}
		copy = append(copy, rs)
		return copy, nil
	}
	for _, current := range current {
		switch current := current.(type) {
		case []any:
			{
				rs, err := ExecSelect(query, current)
				if err != nil {
					return nil, err
				}
				copy = append(copy, rs)
			}
		case Map:
			{
				rs, err := SelectExpr(query, current, &query.selectDefinition)
				if err != nil {
					return nil, err
				}
				copy = append(copy, rs)
			}
		default:
			{
				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `SELECT` statement. cannot select from %T", current))
			}
		}
	}
	return copy, nil
}

func ExecDistinct(query *Query, current []any) ([]any, error) {
	if !query.distinct {
		return current, nil
	}
	mapper := make(map[string]bool)
	slice := make([]any, 0)
	for _, item := range current {
		sha256 := sha256.New()
		_, err := sha256.Write([]byte(fmt.Sprintf("%v", item)))
		if err != nil {
			return nil, err
		}
		if _, ok := mapper[hex.EncodeToString(sha256.Sum(nil))]; ok {
			continue
		}
		mapper[hex.EncodeToString(sha256.Sum(nil))] = true
		slice = append(slice, item)
	}
	return slice, nil
}

func ExecOrderBy(query *Query, current []any) ([]any, error) {
	if query.orderByDefinition == nil {
		return current, nil
	}
	err := Sort(current, query.orderByDefinition)
	if err != nil {
		return nil, err
	}
	return current, nil
}

func (query *Query) exec() (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	if query.dual {
		rs, err := ExecSelect(query, query.from)
		if err != nil {
			return nil, err
		}
		if len(rs) == 0 {
			return nil, nil
		}
		return rs[0], nil
	}
	slice := make([]any, 0)
	for _, current := range query.from {
		switch current := current.(type) {
		case []any:
			{
				copy := CopyQuery(query)
				copy.from = current
				rs, err := copy.exec()
				if err != nil {
					return nil, err
				}
				slice = append(slice, rs)
			}
		case Map:
			{
				isMatch, err := ExecWhere(query, current)
				if err != nil {
					return nil, err
				}
				if !isMatch {
					continue
				}
				slice = append(slice, current)
			}
		}
	}
	rs, err := ExecGroupBy(query, slice)
	if err != nil {
		return nil, err
	}
	//query.processed = rs
	offset := 0
	if query.offsetDefinition != -1 {
		offset = query.offsetDefinition
	}
	rs, err = ExecSelect(query, rs)
	if err != nil {
		return nil, err
	}
	rs, err = ExecDistinct(query, rs)
	if err != nil {
		return nil, err
	}
	rs, err = ExecOrderBy(query, rs)
	if err != nil {
		return nil, err
	}
	limit := len(rs)
	if query.limitDefinition != -1 {
		limit = query.limitDefinition
	}
	if offset >= len(rs) {
		rs = nil
		goto FINALIZE
	}
	if limit >= len(rs) {
		limit = len(rs)
	}
	rs = rs[offset:][:limit]
FINALIZE:
	if query.options.completed != nil {
		query.options.completed()
	}
	return rs, nil
}

func (query *Query) execAndPostProcess() (result any, err error) {
	rs, err := query.exec()
	if err != nil {
		return nil, err
	}
	query.wg.Wait()
	for _, postProcessor := range query.postProcessors {
		err := postProcessor()
		if err != nil {
			return nil, err
		}
	}
	return rs, nil
}

func (query *Query) Exec() (result []any, err error) {
	rs, err := query.execAndPostProcess()
	if err != nil {
		return nil, err
	}
	if slice, ok := rs.([]any); ok {
		return slice, nil
	}
	return []any{rs}, nil
}

func (query *Query) IsDual() bool {
	return query.dual
}

func RegexComparison(left any, pattern string) (bool, error) {
	regExpr := strings.ReplaceAll(strings.ToLower(pattern), "_", ".")
	regExpr = strings.ReplaceAll(regExpr, "%", ".*")
	regExpr = "^" + regExpr + "$"
	return regexp.Match(regExpr, []byte(strings.ToLower(fmt.Sprintf("%v", left))))
}

func RegisterFunction(name string, function Function) {
	if functions == nil {
		functions = make(map[string]Function)
		immediateFunctions = make([]string, 0)
	}
	functions[strings.ToLower(name)] = function
}

func RegisterImmediateFunction(name string, function Function) {
	RegisterFunction(name, function)
	immediateFunctions = append(immediateFunctions, strings.ToLower(name))
}

func RegisterExternalFunction(name string, function func([]any) (any, error)) {
	if functions == nil {
		functions = make(map[string]Function)
		immediateFunctions = make([]string, 0)
	}
	functions[strings.ToLower(name)] = func(_ *Query, _ Map, _ *FunctionOptions, args []any) (any, error) {
		return function(args)
	}
}

func Import(functions map[string]func([]any) (any, error)) {
	for name, function := range functions {
		RegisterExternalFunction(name, function)
	}
}

func CopyQuery(query *Query) *Query {
	return &Query{
		data:              query.data,
		from:              query.from,
		groupDefinition:   query.groupDefinition,
		havingDefinition:  query.havingDefinition,
		whereDefinition:   query.havingDefinition,
		selectDefinition:  query.selectDefinition,
		limitDefinition:   query.limitDefinition,
		offsetDefinition:  query.offsetDefinition,
		orderByDefinition: query.orderByDefinition,
		options:           query.options,
		postProcessors:    query.postProcessors,
	}
}

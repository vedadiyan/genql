// Copyright 2023 Pouya Vedadiyan

// Licensed under the Apache License, Version 2.0 (the "License");

// you may not use this file except in compliance with the License.

// You may obtain a copy of the License at

//

//     http://www.apache.org/licenses/LICENSE-2.0

//

// Unless required by applicable law or agreed to in writing, software

// distributed under the License is distributed on an "AS IS" BASIS,

// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// See the License for the specific language governing permissions and

// limitations under the License.

package genql

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/vedadiyan/sqlparser/v2"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		data      Map
		query     string
		options   []QueryOption
		wantErr   bool
		checkFunc func(*Query) bool
	}{
		{
			name: "Basic Query Creation",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   "SELECT * FROM test",
			options: nil,
			wantErr: false,
			checkFunc: func(q *Query) bool {
				data := q.data["test"]
				if data == nil {
					return false
				}
				testData, ok := data.([]Map)
				return q != nil && ok && len(testData) == 2 && testData[0]["id"] == 1
			},
		},
		{
			name: "With Wrapped Option",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   "SELECT * FROM test",
			options: []QueryOption{Wrapped()},
			wantErr: false,
			checkFunc: func(q *Query) bool {
				root, ok := q.data["root"].(Map)
				if !ok {
					return false
				}
				testData, ok := root["test"].([]Map)
				return q != nil && q.options.wrapped && ok && len(testData) == 2 && testData[0]["id"] == 1
			},
		},
		{
			name: "With Postgres Dialect",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   `SELECT * FROM "test"`,
			options: []QueryOption{PostgresEscapingDialect()},
			wantErr: false,
			checkFunc: func(q *Query) bool {
				data := q.data["test"]
				if data == nil {
					return false
				}
				testData, ok := data.([]Map)
				return q != nil && q.options.postgresEscapingDialect && ok && len(testData) == 2
			},
		},
		{
			name: "Invalid SQL Query",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
				},
			},
			query:     "INVALID SQL",
			options:   nil,
			wantErr:   true,
			checkFunc: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := New(tt.data, tt.query, tt.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.checkFunc(query) {
				t.Errorf("New() failed validation check")
			}
		})
	}
}
func TestQueryOptions(t *testing.T) {
	tests := []struct {
		name  string
		opt   QueryOption
		check func(*Query) bool
	}{
		{
			name: "Wrapped Option",
			opt:  Wrapped(),
			check: func(q *Query) bool {
				return q.options.wrapped
			},
		},
		{
			name: "Postgres Dialect Option",
			opt:  PostgresEscapingDialect(),
			check: func(q *Query) bool {
				return q.options.postgresEscapingDialect
			},
		},
		{
			name: "Idiomatic Arrays Option",
			opt:  IdomaticArrays(),
			check: func(q *Query) bool {
				return q.options.idomaticArrays
			},
		},
		{
			name: "With Constants Option",
			opt:  WithConstants(map[string]any{"const": "value"}),
			check: func(q *Query) bool {
				return q.options.constants["const"] == "value"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query{options: &Options{}}
			tt.opt(q)
			if !tt.check(q) {
				t.Errorf("Option %s failed to set correct value", tt.name)
			}
		})
	}
}

func TestExecSelect(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Select All Fields",
			query: &Query{
				selectDefinition: sqlparser.SelectExprs{
					Exprs: []sqlparser.SelectExpr{
						&sqlparser.StarExpr{},
					},
				},
			},
			input: []any{
				Map{"id": 1, "name": "test"},
			},
			want: []any{
				Map{"id": 1, "name": "test"},
			},
			wantErr: false,
		},
		{
			name: "Select Specific Fields",
			query: &Query{
				selectDefinition: sqlparser.SelectExprs{
					Exprs: []sqlparser.SelectExpr{
						&sqlparser.AliasedExpr{
							Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
						},
					},
				},
			},
			input: []any{
				Map{"id": 1, "name": "test"},
			},
			want: []any{
				Map{"id": 1},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecSelect(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Order By Single Field Ascending",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "id", Value: true},
				},
			},
			input: []any{
				Map{"id": 2.0},
				Map{"id": 1.0},
				Map{"id": 3.0},
			},
			want: []any{
				Map{"id": 1.0},
				Map{"id": 2.0},
				Map{"id": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Order By Single Field Descending",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "id", Value: false},
				},
			},
			input: []any{
				Map{"id": 2.0},
				Map{"id": 1.0},
				Map{"id": 3.0},
			},
			want: []any{
				Map{"id": 3.0},
				Map{"id": 2.0},
				Map{"id": 1.0},
			},
			wantErr: false,
		},
		{
			name: "Order By Multiple Fields Ascending",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "age", Value: true},
					{Key: "name", Value: true},
				},
			},
			input: []any{
				Map{"age": 30.0, "name": "Bob"},
				Map{"age": 25.0, "name": "Alice"},
				Map{"age": 25.0, "name": "Charlie"},
			},
			want: []any{
				Map{"age": 25.0, "name": "Alice"},
				Map{"age": 25.0, "name": "Charlie"},
				Map{"age": 30.0, "name": "Bob"},
			},
			wantErr: false,
		},
		{
			name: "Order By Mixed Directions",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "age", Value: true},
					{Key: "salary", Value: false},
				},
			},
			input: []any{
				Map{"age": 30.0, "salary": 50000.0},
				Map{"age": 25.0, "salary": 45000.0},
				Map{"age": 25.0, "salary": 55000.0},
			},
			want: []any{
				Map{"age": 25.0, "salary": 55000.0},
				Map{"age": 25.0, "salary": 45000.0},
				Map{"age": 30.0, "salary": 50000.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecOrderBy(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecOrderBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecOrderBy() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestExecWhere(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		current Map
		want    bool
		wantErr bool
	}{
		{
			name: "Simple Equality Check",
			query: &Query{
				whereDefinition: &sqlparser.Where{
					Type: sqlparser.WhereClause,
					Expr: &sqlparser.ComparisonExpr{
						Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
						Right:    sqlparser.NewIntLiteral("1"),
						Operator: sqlparser.EqualOp,
					},
				},
			},
			current: Map{"id": 1},
			want:    true,
			wantErr: false,
		},
		{
			name: "Complex AND Condition",
			query: &Query{
				whereDefinition: &sqlparser.Where{
					Type: sqlparser.WhereClause,
					Expr: &sqlparser.AndExpr{
						Left: &sqlparser.ComparisonExpr{
							Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
							Right:    sqlparser.NewIntLiteral("1"),
							Operator: sqlparser.EqualOp,
						},
						Right: &sqlparser.ComparisonExpr{
							Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("active")},
							Right:    sqlparser.BoolVal(true),
							Operator: sqlparser.EqualOp,
						},
					},
				},
			},
			current: Map{"id": 1, "active": true},
			want:    true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecWhere(tt.query, tt.current)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecWhere() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ExecWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Group By Single Field",
			query: &Query{
				groupDefinition: GroupDefinition{
					"category": true,
				},
			},
			input: []any{
				Map{"category": "A", "value": 1},
				Map{"category": "A", "value": 2},
				Map{"category": "B", "value": 3},
			},
			want: []any{
				Map{
					"category": "A",
					"*": []any{
						Map{"category": "A", "value": 1},
						Map{"category": "A", "value": 2},
					},
				},
				Map{
					"category": "B",
					"*": []any{
						Map{"category": "B", "value": 3},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecGroupBy(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecGroupBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecGroupBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_Exec(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []any
		wantErr bool
	}{
		{
			name:  "Simple Select All",
			query: "SELECT * FROM users",
			data: Map{
				"users": []any{
					Map{"id": 1, "name": "John"},
					Map{"id": 2, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 1, "name": "John"},
				Map{"id": 2, "name": "Jane"},
			},
			wantErr: false,
		},
		{
			name:  "Select With Where Clause",
			query: "SELECT * FROM users WHERE id = 1",
			data: Map{
				"users": []any{
					Map{"id": 1, "name": "John"},
					Map{"id": 2, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 1, "name": "John"},
			},
			wantErr: false,
		},
		{
			name:  "Select With Order By",
			query: "SELECT * FROM users ORDER BY id DESC",
			data: Map{
				"users": []any{
					Map{"id": 1.0, "name": "John"},
					Map{"id": 2.0, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 2.0, "name": "Jane"},
				Map{"id": 1.0, "name": "John"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if err != nil {
				t.Fatalf("Failed to create query: %v", err)
			}

			got, err := q.Exec()
			if (err != nil) != tt.wantErr {
				t.Errorf("Query.Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query.Exec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildCte(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		wantErr bool
	}{
		{
			name:  "Simple CTE",
			query: `WITH temp AS (SELECT * FROM test WHERE id = 1) SELECT * FROM temp`,
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			wantErr: false,
		},
		{
			name: "Multiple CTEs",
			query: `
				WITH temp1 AS (SELECT * FROM test WHERE id = 1),
				     temp2 AS (SELECT * FROM test WHERE id = 2)
				SELECT * FROM temp1 UNION SELECT * FROM temp2`,
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildCte() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestBuildJoin(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Inner Join",
			query: `SELECT u.*, o.amount 
					FROM users u 
					HASH_JOIN orders o ON o.user_id = u.id AND  o.user_id = u.id AND  o.user_id = u.id`,
			data: Map{
				"users": []Map{
					{"id": 1, "name": "user1"},
					{"id": 2, "name": "user2"},
				},
				"orders": []Map{
					{"id": 1, "user_id": 1, "amount": 100},
					{"id": 2, "user_id": 1, "amount": 200},
				},
			},
			want: []Map{
				{"id": 1, "name": "user1", "amount": 100},
				{"id": 1, "name": "user1", "amount": 200},
			},
			wantErr: false,
		},
		{
			name: "Left Join",
			query: `SELECT u.*, o.amount 
					FROM users u 
					LEFT JOIN orders o ON u.id = o.user_id`,
			data: Map{
				"users": []Map{
					{"id": 1, "name": "user1"},
					{"id": 2, "name": "user2"},
				},
				"orders": []Map{
					{"id": 1, "user_id": 1, "amount": 100},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildJoin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestAggregations(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Count with Group By",
			query: `SELECT category, COUNT(*) as count
					FROM test
					GROUP BY category
					ORDER BY category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 1},
					{"category": "A", "value": 2},
					{"category": "B", "value": 3},
				},
			},
			want: []Map{
				{"category": "A", "count": 2},
				{"category": "B", "count": 1},
			},
			wantErr: false,
		},
		{
			name: "Sum with Having",
			query: `SELECT category, SUM(value) as total
					FROM test
					GROUP BY category
					HAVING SUM(value) > 2
					ORDER BY category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 10},
					{"category": "A", "value": 2},
					{"category": "B", "value": 3},
				},
			},
			want: []Map{
				{"category": "A", "total": 12},
				{"category": "B", "total": 3},
			},
			wantErr: false,
		},
		{
			name: "Multiple Aggregations",
			query: `SELECT category,
					COUNT(*) as count,
					SUM(value) as total,
					AVG(value) as avg,
					MIN(value) as min,
					MAX(value) as max
					FROM test
					GROUP BY category
					ORDER BY category DESC`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 10},
					{"category": "A", "value": 20},
					{"category": "B", "value": 30},
				},
			},
			want: []Map{
				{"category": "B", "count": 1, "total": 30, "avg": 30, "min": 30, "max": 30},
				{"category": "A", "count": 2, "total": 30, "avg": 15, "min": 10, "max": 20},
			},
			wantErr: false,
		},
		{
			name: "Group By Multiple Columns",
			query: `SELECT category, status, COUNT(*) as count
					FROM test
					GROUP BY category, status
					ORDER BY category, status`,
			data: Map{
				"test": []Map{
					{"category": "A", "status": "active", "value": 1},
					{"category": "A", "status": "active", "value": 2},
					{"category": "A", "status": "inactive", "value": 3},
					{"category": "B", "status": "active", "value": 4},
				},
			},
			want: []Map{
				{"category": "A", "status": "active", "count": 2},
				{"category": "A", "status": "inactive", "count": 1},
				{"category": "B", "status": "active", "count": 1},
			},
			wantErr: false,
		},
		{
			name: "Having with Multiple Conditions",
			query: `SELECT category, COUNT(*) as count, SUM(value) as total
					FROM test
					GROUP BY category
					HAVING COUNT(*) > 1 AND SUM(value) > 10
					ORDER BY total DESC, category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 8},
					{"category": "A", "value": 4},
					{"category": "B", "value": 15},
				},
			},
			want: []Map{
				{"category": "A", "count": 2, "total": 12},
			},
			wantErr: false,
		},
		{
			name: "Null Value Handling",
			query: `SELECT category, COUNT(*) as count, SUM(value) as total
					FROM test
					GROUP BY category
					ORDER BY category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": nil},
					{"category": "A", "value": 2},
					{"category": "B", "value": 3},
				},
			},
			want: []Map{
				{"category": "A", "count": 2, "total": 2},
				{"category": "B", "count": 1, "total": 3},
			},
			wantErr: false,
		},
		{
			name: "Empty Group",
			query: `SELECT category, COUNT(*) as count
					FROM test
					GROUP BY category
					ORDER BY category`,
			data: Map{
				"test": []Map{},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "Order By Multiple Aggregates",
			query: `SELECT category, COUNT(*) as count, SUM(value) as total
					FROM test
					GROUP BY category
					ORDER BY count DESC, total ASC`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 5},
					{"category": "A", "value": 3},
					{"category": "B", "value": 10},
					{"category": "C", "value": 15},
				},
			},
			want: []Map{
				{"category": "A", "count": 2, "total": 8},
				{"category": "B", "count": 1, "total": 10},
				{"category": "C", "count": 1, "total": 15},
			},
			wantErr: false,
		},
		{
			name: "Invalid Aggregate Function",
			query: `SELECT category, INVALID(value) as result
					FROM test
					GROUP BY category
					ORDER BY category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 1},
				},
			},
			wantErr: true,
		},
		{
			name: "Missing Group By Column",
			query: `SELECT category, COUNT(*) as count
					FROM test
					GROUP BY missing_column
					ORDER BY missing_column`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 1},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if err != nil {
					t.Errorf("Exec() error = %v", err)
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestAggregatesFunctions(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Count All",
			query: `SELECT COUNT(*) as count
					FROM test
					ORDER BY count`,
			data: Map{
				"test": []Map{
					{"value": 1},
					{"value": 2},
					{"value": 3},
				},
			},
			want: []Map{
				{"count": 3},
			},
			wantErr: false,
		},
		{
			name: "Sum With Nulls",
			query: `SELECT SUM(value) as total
					FROM test
					ORDER BY total`,
			data: Map{
				"test": []Map{
					{"value": 10},
					{"value": nil},
					{"value": 5},
				},
			},
			want: []Map{
				{"total": 15},
			},
			wantErr: false,
		},
		{
			name: "Nested",
			query: `SELECT (SELECT SUM(value) as total FROM value) AS totals
					FROM test
					ORDER BY total`,
			data: Map{
				"test": []Map{
					{"value": []Map{{"value": 1}, {"value": 2}, {"value": 3}}},
					{"value": []Map{{"value": 4}, {"value": 5}, {"value": 6}}},
					{"value": []Map{{"value": 7}, {"value": 8}, {"value": 9}}},
				},
			},
			want: []Map{
				{"totals": []Map{{"total": 6}}},
				{"totals": []Map{{"total": 15}}},
				{"totals": []Map{{"total": 24}}},
			},
			wantErr: false,
		},
		{
			name: "Multiple Aggregates",
			query: `SELECT
					COUNT(*) as count,
					SUM(value) as total,
					AVG(value) as avg,
					MIN(value) as min,
					MAX(value) as max
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 10},
					{"value": 20},
					{"value": 30},
				},
			},
			want: []Map{
				{"count": 3, "total": 60, "avg": 20, "min": 10, "max": 30},
			},
			wantErr: false,
		},
		{
			name: "Empty Table",
			query: `SELECT COUNT(*) as count, SUM(value) as total
					FROM test`,
			data: Map{
				"test": []Map{},
			},
			want: []Map{
				{"count": 0, "total": nil},
			},
			wantErr: false,
		},
		{
			name: "Invalid Function",
			query: `SELECT INVALID(value) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 1},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}
func TestExpressions(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		wantErr bool
	}{
		{
			name: "CASE Expression",
			query: `SELECT id, 
					CASE 
						WHEN value > 2 THEN 'High'
						ELSE 'Low'
					END as category
					FROM test`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 1.0},
					{"id": 2, "value": 3.0},
				},
			},
			wantErr: false,
		},
		{
			name: "Complex WHERE with AND/OR",
			query: `SELECT * FROM test 
					WHERE (value > 1 AND value < 4) 
					OR category = 'A'`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 2.0, "category": "B"},
					{"id": 2, "value": 5.0, "category": "A"},
				},
			},
			wantErr: false,
		},
		{
			name: "Mathematical Expressions",
			query: `SELECT id, value * 2 as doubled,
					value + 1 as increased
					FROM test`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 10.0},
					{"id": 2, "value": 20.0},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestBinaryExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Addition",
			query: `SELECT 5.5 + 2.5 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 8.0},
			},
			wantErr: false,
		},
		{
			name: "Subtraction",
			query: `SELECT 10.0 - 3.0 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 7.0},
			},
			wantErr: false,
		},
		{
			name: "Multiplication",
			query: `SELECT 4.0 * 3.0 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 12.0},
			},
			wantErr: false,
		},
		{
			name: "Division",
			query: `SELECT 15.0 / 3.0 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 5.0},
			},
			wantErr: false,
		},
		{
			name: "Integer Division",
			query: `SELECT 7 DIV 2 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Modulo",
			query: `SELECT 7 % 4 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Bitwise AND",
			query: `SELECT 12 & 10 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 8.0}, // 1100 & 1010 = 1000
			},
			wantErr: false,
		},
		{
			name: "Bitwise OR",
			query: `SELECT 12 | 10 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 14.0}, // 1100 | 1010 = 1110
			},
			wantErr: false,
		},
		{
			name: "Bitwise XOR",
			query: `SELECT 12 ^ 10 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 6.0}, // 1100 ^ 1010 = 0110
			},
			wantErr: false,
		},
		{
			name: "Left Shift",
			query: `SELECT 8 >> 2 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 2.0},
			},
			wantErr: false,
		},
		{
			name: "Right Shift",
			query: `SELECT 2 << 2 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 8.0},
			},
			wantErr: false,
		},
		{
			name: "Division by Zero",
			query: `SELECT 10 / 0 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			wantErr: true,
		},
		{
			name: "Complex Expression",
			query: `SELECT (5 + 3) * 2 as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": 16.0},
			},
			wantErr: false,
		},
		{
			name: "Multiple Operations",
			query: `SELECT 
					10 + 5 as addition,
					10 - 5 as subtraction,
					10 * 5 as multiplication,
					10 / 5 as division
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{
					"addition":       15.0,
					"subtraction":    5.0,
					"multiplication": 50.0,
					"division":       2.0,
				},
			},
			wantErr: false,
		},
		{
			name: "Operations with Column Values",
			query: `SELECT 
					value + 5 as addition,
					value - 5 as subtraction,
					value * 2 as multiplication,
					value / 2 as division
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 10.0},
				},
			},
			want: []Map{
				{
					"addition":       15.0,
					"subtraction":    5.0,
					"multiplication": 20.0,
					"division":       5.0,
				},
			},
			wantErr: false,
		},
		{
			name: "Nested Complex Expression",
			query: `SELECT ((value + 5) * 2) / (4 - value) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 2.0},
				},
			},
			want: []Map{
				{"result": 7.0}, // ((2 + 5) * 2) / (4 - 2) = 14 / 2 = 7
			},
			wantErr: false,
		},
		{
			name: "Operation with Null Value",
			query: `SELECT value + 5 as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			want: []Map{
				{"result": nil},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestIsExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "IS NULL with null value",
			query: `SELECT value IS NULL as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS NULL with non-null value",
			query: `SELECT value IS NULL as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS NOT NULL with null value",
			query: `SELECT value IS NOT NULL as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS NOT NULL with non-null value",
			query: `SELECT value IS NOT NULL as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS TRUE with true value",
			query: `SELECT value IS TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS TRUE with false value",
			query: `SELECT value IS TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": false},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS TRUE with non-boolean value",
			query: `SELECT value IS TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42},
				},
			},
			wantErr: true,
		},
		{
			name: "IS NOT TRUE with true value",
			query: `SELECT value IS NOT TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS NOT TRUE with false value",
			query: `SELECT value IS NOT TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": false},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS FALSE with true value",
			query: `SELECT value IS FALSE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS FALSE with false value",
			query: `SELECT value IS FALSE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": false},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS NOT FALSE with true value",
			query: `SELECT value IS NOT FALSE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "IS NOT FALSE with false value",
			query: `SELECT value IS NOT FALSE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": false},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "IS TRUE with null value",
			query: `SELECT value IS TRUE as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			wantErr: true,
		},
		{
			name: "Multiple IS expressions",
			query: `SELECT 
					value1 IS NULL as null_check,
					value2 IS NOT NULL as not_null_check,
					value3 IS TRUE as true_check,
					value4 IS FALSE as false_check
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"value1": nil,
						"value2": 42,
						"value3": true,
						"value4": false,
					},
				},
			},
			want: []Map{
				{
					"null_check":     true,
					"not_null_check": true,
					"true_check":     true,
					"false_check":    true,
				},
			},
			wantErr: false,
		},
		{
			name: "WHERE clause with IS",
			query: `SELECT value 
					FROM test
					WHERE other_value IS NOT NULL`,
			data: Map{
				"test": []Map{
					{"value": 1, "other_value": 42},
					{"value": 2, "other_value": nil},
					{"value": 3, "other_value": 43},
				},
			},
			want: []Map{
				{"value": 1},
				{"value": 3},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestNotExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "NOT true",
			query: `SELECT NOT true as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "NOT false",
			query: `SELECT NOT false as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "NOT with column boolean value",
			query: `SELECT NOT value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "NOT with comparison",
			query: `SELECT NOT (value > 5) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 3},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "NOT with NULL value",
			query: `SELECT NOT value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			wantErr: true,
		},
		{
			name: "NOT with non-boolean value",
			query: `SELECT NOT value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42},
				},
			},
			wantErr: true,
		},
		{
			name: "NOT in WHERE clause",
			query: `SELECT value 
					FROM test 
					WHERE NOT (value < 10)`,
			data: Map{
				"test": []Map{
					{"value": 5},
					{"value": 15},
					{"value": 8},
				},
			},
			want: []Map{
				{"value": 15},
			},
			wantErr: false,
		},
		{
			name: "Multiple NOT conditions",
			query: `SELECT 
					NOT (value1 > 0) as cond1,
					NOT (value2 < 10) as cond2
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"value1": -5,
						"value2": 15,
					},
				},
			},
			want: []Map{
				{
					"cond1": true,
					"cond2": true,
				},
			},
			wantErr: false,
		},
		{
			name: "Nested NOT expressions",
			query: `SELECT NOT NOT value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": true},
			},
			wantErr: false,
		},
		{
			name: "NOT with complex boolean expression",
			query: `SELECT NOT (value1 > 0 AND value2 < 10) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"value1": 5,
						"value2": 5,
					},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "NOT with missing column",
			query: `SELECT NOT missing_column as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestSubStrExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Basic substring",
			query: `SELECT SUBSTR('hello world', 0, 5) as result
					FROM test`,
			data: Map{
				"test": []Map{{"dummy": 1}},
			},
			want: []Map{
				{"result": "hello"},
			},
			wantErr: false,
		},
		{
			name: "Substring from column value",
			query: `SELECT SUBSTR(text, 0, 3) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"text": "testing"},
				},
			},
			want: []Map{
				{"result": "tes"},
			},
			wantErr: false,
		},
		{
			name: "Substring with numeric from and to",
			query: `SELECT SUBSTR(text, from_pos, length) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"text":     "hello world",
						"from_pos": 6.0,
						"length":   5.0,
					},
				},
			},
			want: []Map{
				{"result": "world"},
			},
			wantErr: false,
		},
		{
			name: "Substring with NULL input string",
			query: `SELECT SUBSTR(text, 0, 5) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"text": nil},
				},
			},
			wantErr: true,
		},
		{
			name: "Substring with NULL from position",
			query: `SELECT SUBSTR(text, pos, 5) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"text": "hello",
						"pos":  nil,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Substring with NULL length",
			query: `SELECT SUBSTR(text, 0, length) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"text":   "hello",
						"length": nil,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Substring with non-string input",
			query: `SELECT SUBSTR(value, 0, 5) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 12345},
				},
			},
			wantErr: true,
		},
		{
			name: "Substring with non-numeric position",
			query: `SELECT SUBSTR('hello', pos, 5) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"pos": "invalid"},
				},
			},
			wantErr: true,
		},
		{
			name: "Substring with non-numeric length",
			query: `SELECT SUBSTR('hello', 0, length) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"length": "invalid"},
				},
			},
			wantErr: true,
		},
		{
			name: "Multiple substring operations",
			query: `SELECT
					SUBSTR(text, 0, 3) as first,
					SUBSTR(text, 3, 3) as second,
					SUBSTR(text, 6, 3) as third
					FROM test`,
			data: Map{
				"test": []Map{
					{"text": "hello world"},
				},
			},
			want: []Map{
				{
					"first":  "hel",
					"second": "lo ",
					"third":  "wor",
				},
			},
			wantErr: false,
		},
		{
			name: "Substring in WHERE clause",
			query: `SELECT text
					FROM test
					WHERE SUBSTR(text, 0, 5) = 'hello'`,
			data: Map{
				"test": []Map{
					{"text": "hello world"},
					{"text": "goodbye world"},
				},
			},
			want: []Map{
				{"text": "hello world"},
			},
			wantErr: false,
		},
		{
			name: "Substring with computed positions",
			query: `SELECT SUBSTR(text, pos1 + pos2, len1 + len2) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"text": "hello world",
						"pos1": 1.0,
						"pos2": 2.0,
						"len1": 2.0,
						"len2": 3.0,
					},
				},
			},
			want: []Map{
				{"result": "lo wo"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestUnaryExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Bitwise NOT (~)",
			query: `SELECT ~ value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 5.0}, // 5 in binary: 101
				},
			},
			want: []Map{
				{"result": -6.0}, // ~5 = -6 (two's complement)
			},
			wantErr: false,
		},
		{
			name: "Unary minus (-)",
			query: `SELECT -value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42.0},
				},
			},
			want: []Map{
				{"result": -42.0},
			},
			wantErr: false,
		},
		{
			name: "Logical NOT (!)",
			query: `SELECT !value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": true},
				},
			},
			want: []Map{
				{"result": false},
			},
			wantErr: false,
		},
		{
			name: "Unary minus with expression",
			query: `SELECT -(value + 10) as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 5.0},
				},
			},
			want: []Map{
				{"result": -15.0},
			},
			wantErr: false,
		},
		{
			name: "Multiple unary operators",
			query: `SELECT 
					-value as neg,
					~value as bitnot,
					!flag as lognot
					FROM test`,
			data: Map{
				"test": []Map{
					{
						"value": 10.0,
						"flag":  true,
					},
				},
			},
			want: []Map{
				{
					"neg":    -10.0,
					"bitnot": -11.0, // ~10 = -11 (two's complement)
					"lognot": false,
				},
			},
			wantErr: false,
		},
		{
			name: "Unary with NULL value",
			query: `SELECT -value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": nil},
				},
			},
			wantErr: true,
		},
		{
			name: "Bitwise NOT with non-numeric",
			query: `SELECT ~value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": "invalid"},
				},
			},
			wantErr: true,
		},
		{
			name: "Logical NOT with non-boolean",
			query: `SELECT !value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42.0},
				},
			},
			wantErr: true,
		},
		{
			name: "Unary in WHERE clause",
			query: `SELECT value
					FROM test
					WHERE -value < -10`,
			data: Map{
				"test": []Map{
					{"value": 5.0},
					{"value": 15.0},
					{"value": 20.0},
				},
			},
			want: []Map{
				{"value": 15.0},
				{"value": 20.0},
			},
			wantErr: false,
		},
		{
			name: "Nested unary operators",
			query: `SELECT --value as result
					FROM test`,
			data: Map{
				"test": []Map{
					{"value": 42.0},
				},
			},
			want: []Map{
				{"result": 42.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

func TestValueTupleExpr(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Simple value tuple in IN clause",
			query: `SELECT value
					FROM test
					WHERE value IN (1, 2, 3)`,
			data: Map{
				"test": []Map{
					{"value": 1.0},
					{"value": 2.0},
					{"value": 4.0},
					{"value": 5.0},
				},
			},
			want: []Map{
				{"value": 1.0},
				{"value": 2.0},
			},
			wantErr: false,
		},
		{
			name: "Value tuple with different types",
			query: `SELECT value
					FROM test
					WHERE str IN ('apple', 'banana', 'orange')`,
			data: Map{
				"test": []Map{
					{"str": "apple", "value": 1.0},
					{"str": "grape", "value": 2.0},
					{"str": "banana", "value": 3.0},
				},
			},
			want: []Map{
				{"value": 1.0},
				{"value": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Value tuple with expressions",
			query: `SELECT value
					FROM test
					WHERE value IN (1+2, 2*2, 10-3)`,
			data: Map{
				"test": []Map{
					{"value": 3.0},
					{"value": 4.0},
					{"value": 7.0},
					{"value": 8.0},
				},
			},
			want: []Map{
				{"value": 3.0},
				{"value": 4.0},
				{"value": 7.0},
			},
			wantErr: false,
		},
		{
			name: "Value tuple with column references",
			query: `SELECT value
					FROM test
					WHERE value IN (val1, val2, val3)`,
			data: Map{
				"test": []Map{
					{"value": 1.0, "val1": 1.0, "val2": 2.0, "val3": 3.0},
					{"value": 4.0, "val1": 1.0, "val2": 2.0, "val3": 3.0},
				},
			},
			want: []Map{
				{"value": 1.0},
			},
			wantErr: false,
		},
		{
			name: "Value tuple with NULL values",
			query: `SELECT value
					FROM test
					WHERE value IN (1, NULL, 3)`,
			data: Map{
				"test": []Map{
					{"value": 1.0},
					{"value": 2.0},
					{"value": 3.0},
				},
			},
			want: []Map{
				{"value": 1.0},
				{"value": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Value tuple with subquery",
			query: `SELECT value 
					FROM "wrapper.test" 
					WHERE value IN (SELECT id FROM "<-wrapper.other_table")`,
			data: Map{
				"wrapper": Map{
					"test": []Map{
						{"value": 1.0},
						{"value": 2.0},
						{"value": 3.0},
					},
					"other_table": []Map{
						{"id": 1.0},
						{"id": 3.0},
					},
				},
			},
			want: []Map{
				{"value": 1.0},
				{"value": 3.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if err != nil {
				t.Errorf("New() error = %v", err)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.want) {
					t.Errorf("Exec() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}

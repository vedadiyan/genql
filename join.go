package genql

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/vedadiyan/sqlparser/v2"
)

type (
	Join struct {
		query                 *Query
		left, right           []any
		leftIdent, rightIdent string
		into                  string
		joinExpr              sqlparser.Expr
		joinType              sqlparser.JoinType
	}
	HashedTable struct {
		Rows map[string][]*any
		Keys map[string]*Map
	}
)

func ToHash(bytes []byte) (string, error) {
	sha256 := sha256.New()
	_, err := sha256.Write(bytes)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sha256.Sum(nil)), nil
}

func NewHashedTable() *HashedTable {
	out := new(HashedTable)
	out.Rows = make(map[string][]*any)
	out.Keys = make(map[string]*map[string]any)

	return out
}

func ToCatalog(rows []any, ident string, identRight string, joinExpr sqlparser.Expr) (*HashedTable, error) {
	hashedTable := NewHashedTable()
	var buffer bytes.Buffer
	columns := extractJoinColumns(ident, identRight, joinExpr)
	sort.Slice(columns, func(i, j int) bool {
		return columns[i] > columns[j]
	})
	mappedColumns := make(map[string]string)
	for _, column := range columns {
		mappedColumns[column] = strings.ReplaceAll(column, "'", "")
	}
	for _, row := range rows {
		r := row
		mapper := make(Map)
		buffer.Reset()
		for _, column := range columns {
			reader, err := ExecReader(row, column)
			if err != nil {
				return nil, err
			}
			binary.Write(&buffer, binary.LittleEndian, reader)
			buffer.WriteString("-")
			mapper[mappedColumns[column]] = reader
		}
		hash, err := ToHash(buffer.Bytes())
		if err != nil {
			return nil, err
		}
		if _, ok := hashedTable.Keys[hash]; !ok {
			hashedTable.Rows[hash] = make([]*any, 0)
			hashedTable.Keys[hash] = &mapper
		}
		hashedTable.Rows[hash] = append(hashedTable.Rows[hash], &r)
	}
	return hashedTable, nil
}

func NewJoin(query *Query, left, right []any, leftIdent, rightIdent string, into string, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) *Join {
	join := new(Join)
	join.query = query
	join.left, join.right = left, right
	join.leftIdent, join.rightIdent = leftIdent, rightIdent
	join.into = into
	join.joinExpr = joinExpr
	join.joinType = joinType
	return join
}

func (j *Join) Exec() ([]any, error) {
	if j.joinType.IsHashJoin() || hashJoinAnalyze(j.leftIdent, j.rightIdent, j.joinExpr) {
		return j.HashJoin()
	}
	return j.StraightJoin()
}

func (j *Join) StraightJoin() ([]any, error) {
	if !j.joinType.IsLeftJoin() {
		j.left, j.right = j.right, j.left
	}

	l, err := ToCatalog(j.left, j.leftIdent, j.rightIdent, j.joinExpr)
	if err != nil {
		return nil, err
	}
	r, err := ToCatalog(j.right, j.rightIdent, j.leftIdent, j.joinExpr)
	if err != nil {
		return nil, err
	}
	if !j.joinType.IsParallel() {
		return j.StraightJoinFunc(l, r)
	}
	return j.ParallelHashJoinFunc(l, r)
}

func (j *Join) HashJoin() ([]any, error) {
	if !j.joinType.IsLeftJoin() {
		j.left, j.right = j.right, j.left
	}

	l, err := ToCatalog(j.left, j.leftIdent, j.rightIdent, j.joinExpr)
	if err != nil {
		return nil, err
	}
	r, err := ToCatalog(j.right, j.rightIdent, j.leftIdent, j.joinExpr)
	if err != nil {
		return nil, err
	}
	if !j.joinType.IsParallel() {
		return j.HashJoinFunc(l, r)
	}
	return j.ParallelHashJoinFunc(l, r)
}

func (j *Join) HashJoinFunc(l, r *HashedTable) ([]any, error) {
	slice := make([]any, 0)
	for lk := range l.Rows {
		switch ok, mapper, err := j.HashJoinMatchFunc(lk, l, r); {
		case ok:
			{
				slice = append(slice, mapper)
			}
		case !ok && err != nil:
			{
				return nil, err
			}
		default:
			{
				continue
			}
		}
	}
	return slice, nil
}

func (j *Join) StraightJoinFunc(l, r *HashedTable) ([]any, error) {
	slice := make([]any, 0)
	for lk, lv := range l.Keys {
		switch ok, matches, err := j.StraightJoinMatchFunc(lk, lv, l, r); {
		case ok:
			{
				mut.Lock()
				slice = append(slice, matches...)
				mut.Unlock()
			}
		case !ok && err != nil:
			{
				return nil, err
			}
		default:
			{
				continue
			}
		}
	}
	return slice, nil
}

func (j *Join) ParallelStraightJoinFunc(l, r *HashedTable) ([]any, error) {
	var mut sync.Mutex
	var wg sync.WaitGroup
	slice := make([]any, 0)

	for lk, lv := range l.Keys {
		wg.Add(1)
		go func(lk string, lv *map[string]any) {
			defer wg.Done()
			switch ok, matches, err := j.StraightJoinMatchFunc(lk, lv, l, r); {
			case ok:
				{
					mut.Lock()
					slice = append(slice, matches...)
					mut.Unlock()
				}
			case !ok && err != nil:
				{
					panic(err)
				}
			default:
				{
					break
				}
			}
		}(lk, lv)
	}
	wg.Wait()
	return slice, nil
}

func (j *Join) StraightJoinMatchFunc(lk string, lv *map[string]any, l, r *HashedTable) (bool, []any, error) {
	slice := make([]any, 0)
	b := false
	for rk, rv := range r.Keys {
		_current := make(Map)
		maps.Copy(_current, *lv)
		maps.Copy(_current, *rv)
		rs, err := Expr(j.query, _current, j.joinExpr, HardCodedValueExprOpt())
		if err != nil {
			return false, nil, err
		}
		rsValue, ok := rs.(bool)
		if !ok {
			return false, nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", rsValue))
		}
		if rsValue || !j.joinType.IsInner() {
			b = true
			if len(j.into) != 0 {
				current := make(Map)
				if err := Copy(current, l.Rows[lk], j.leftIdent); err != nil {
					return false, nil, err
				}
				if err := Copy(current, r.Rows[rk], j.rightIdent); err != nil {
					return false, nil, err
				}

				maps.Copy(current, *(l.Keys[lk]))
				maps.Copy(current, *(r.Keys[rk]))
				out := make(Map)
				out[j.into] = current
				slice = append(slice, current)
				continue
			}
			current := make([]any, 0)
			for _, lr := range l.Rows[lk] {
				for _, rr := range r.Rows[rk] {
					mapper := make(Map)
					maps.Copy(mapper, (*lr).(Map))
					maps.Copy(mapper, (*rr).(Map))
					current = append(current, mapper)
				}
			}
			slice = append(slice, current)
		}
	}
	return b, slice, nil
}

func (j *Join) ParallelHashJoinFunc(l, r *HashedTable) ([]any, error) {
	var mut sync.Mutex
	var wg sync.WaitGroup
	slice := make([]any, 0)
	for lk := range l.Rows {
		wg.Add(1)
		go func(lk string) {
			defer wg.Done()
			switch ok, mapper, err := j.HashJoinMatchFunc(lk, l, r); {
			case ok:
				{
					mut.Lock()
					slice = append(slice, mapper)
					mut.Unlock()
				}
			case !ok && err != nil:
				{
					panic(err)
				}
			default:
				{
					break
				}
			}
		}(lk)
	}
	wg.Wait()
	return slice, nil
}

func (j *Join) HashJoinMatchFunc(hash string, l, r *HashedTable) (bool, any, error) {
	if right, ok := r.Rows[hash]; ok || !j.joinType.IsInner() {
		left := l.Rows[hash]
		if len(j.into) != 0 {
			current := make(Map)
			if err := Copy(current, left, j.leftIdent); err != nil {
				return false, nil, err
			}
			if err := Copy(current, right, j.rightIdent); err != nil {
				return false, nil, err
			}

			maps.Copy(current, *(l.Keys[hash]))
			maps.Copy(current, *(r.Keys[hash]))
			out := make(Map)
			out[j.into] = current
			return true, out, nil
		}
		current := make([]any, 0)
		for _, lr := range left {
			for _, rr := range right {
				mapper := make(Map)
				maps.Copy(mapper, (*lr).(Map))
				maps.Copy(mapper, (*rr).(Map))
				current = append(current, mapper)
			}
		}
		return true, current, nil
	}
	return false, nil, nil
}

func Copy(out Map, table []*any, ident string) error {
	switch l := len(ident); {
	case l == 0 && len(table) == 1:
		{
			index := *table[0]
			if index, ok := index.(Map); ok {
				maps.Copy(out, index)
				break
			}
			return fmt.Errorf("expected Map but got %T", index)
		}
	case l != 0:
		{
			tmp := make([]any, 0)
			for _, x := range table {
				if index, ok := (*x).(Map); ok {
					tmp = append(tmp, index[ident])
					continue
				}
				return fmt.Errorf("expected Map but got %T", *x)
			}
			out[ident] = tmp
		}
	default:
		{
			return fmt.Errorf("expectation failed")
		}
	}
	return nil
}

func hashJoinAnalyze(ident string, identRight string, expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		{
			if e.Operator != sqlparser.EqualOp {
				return false
			}
		}
	case *sqlparser.OrExpr:
		{
			return false
		}
	case *sqlparser.AndExpr:
		{
			return hashJoinAnalyze(ident, identRight, e.Left) && hashJoinAnalyze(ident, identRight, e.Right)
		}
	default:
		{
			return false
		}
	}
	return true
}

func extractJoinColumns(ident string, identRight string, expr sqlparser.Expr) []string {
	var columns []string

	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if b, _, leftCol := extractColumnsFromExpr(ident, e.Left); b {
			columns = append(columns, leftCol)
			break
		}

		if b, _, rightCol := extractColumnsFromExpr(ident, e.Right); b {
			columns = append(columns, rightCol)
			break
		}

		b, _, leftCol := extractColumnsFromExpr(identRight, e.Left)
		_, _, rightCol := extractColumnsFromExpr(identRight, e.Right)
		if b {
			columns = append(columns, rightCol)
			break
		}
		columns = append(columns, leftCol)
	case *sqlparser.AndExpr:
		leftCols := extractJoinColumns(ident, identRight, e.Left)
		rightCols := extractJoinColumns(ident, identRight, e.Right)
		columns = append(columns, leftCols...)
		columns = append(columns, rightCols...)
	case *sqlparser.OrExpr:
		leftCols := extractJoinColumns(ident, identRight, e.Left)
		rightCols := extractJoinColumns(ident, identRight, e.Right)
		columns = append(columns, leftCols...)
		columns = append(columns, rightCols...)
	}

	return removeDuplicates(columns)
}

func removeDuplicates(slice []string) []string {
	seen := make(map[string]bool)
	unique := make([]string, 0)
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			unique = append(unique, item)
		}
	}
	return unique
}

func extractColumnsFromExpr(ident string, expr sqlparser.Expr) (bool, string, string) {

	switch e := expr.(type) {
	case *sqlparser.ColName:
		// Handle column references like "table.column" or just "column"
		colName := strings.Split(e.Name.String(), ".")
		if !e.Qualifier.IsEmpty() {
			colName = append([]string{e.Qualifier.Name.String()}, colName...)
		}

		return ident == colName[0], colName[0], strings.Join(colName, ".")
	}

	panic("")
}

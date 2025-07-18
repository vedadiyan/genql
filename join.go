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

// Original nested loop join (for reference)
func ExecJoin2(query *Query, left []any, right []any, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
	if joinType == sqlparser.RightJoinType {
		left, right = right, left
	}
	slice := make([]any, 0)
	for _, left := range left {
		left, ok := left.(Map)
		if !ok {
			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", left))
		}
		joined := false
		for _, right := range right {
			current := make(Map)
			for key, value := range left {
				current[key] = value
			}
			right, ok := right.(Map)
			if !ok {
				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", left))
			}
			for key, value := range right {
				current[key] = value
			}
			rs, err := Expr(query, current, joinExpr, nil)
			if err != nil {
				return nil, err
			}
			rsValue, ok := rs.(bool)
			if !ok {
				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", left))
			}
			if rsValue {
				slice = append(slice, current)
				joined = true
			}
		}
		if !joined {
			current := make(Map)
			for key, value := range left {
				current[key] = value
			}
			if joinType != sqlparser.NormalJoinType {
				slice = append(slice, current)
			}
		}
	}
	return slice, nil
}

func ToHash(bytes []byte) (string, error) {
	sha256 := sha256.New()
	_, err := sha256.Write(bytes)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(sha256.Sum(nil)), nil
}

type (
	HashedTable struct {
		Rows map[string][]*any
		Keys map[string]*Map
	}
)

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

type (
	preference int
	Join       struct {
		query                 *Query
		left, right           []any
		leftIdent, rightIdent string
		into                  string
		joinExpr              sqlparser.Expr
		joinType              sqlparser.JoinType
		options               joinOptions
	}
	joinOptions struct {
		preference preference
	}
	JoinOption func(*joinOptions)
)

func NewJoin(query *Query, left, right []any, leftIdent, rightIdent string, into string, joinExpr sqlparser.Expr, joinType sqlparser.JoinType, opts ...JoinOption) *Join {
	join := new(Join)
	join.query = query
	join.left, join.right = left, right
	join.leftIdent, join.rightIdent = leftIdent, rightIdent
	join.into = into
	join.joinExpr = joinExpr
	join.joinType = joinType
	for _, opt := range opts {
		opt(&join.options)
	}
	return join
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
	return j.StraightJoinFunc(l, r)
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
		for rk, rv := range r.Keys {
			_current := make(Map)
			maps.Copy(_current, *lv)
			maps.Copy(_current, *rv)
			rs, err := Expr(j.query, _current, j.joinExpr, HardCodedValueExprOpt())
			if err != nil {
				return nil, err
			}
			rsValue, ok := rs.(bool)
			if !ok {
				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", rsValue))
			}
			if rsValue || !j.joinType.IsInner() {
				if len(j.into) != 0 {
					current := make(Map)
					if err := Copy(current, l.Rows[lk], j.leftIdent); err != nil {
						return nil, err
					}
					if err := Copy(current, r.Rows[rk], j.rightIdent); err != nil {
						return nil, err
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
	}
	return slice, nil
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

// // HashJoin - Build hash table on smaller relation, probe with larger
// func ExecHashJoin(query *Query, left []any, right []any, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
// 	if joinType == sqlparser.RightJoinType {
// 		left, right = right, left
// 	}

// 	// Choose build and probe sides (build on smaller relation)
// 	buildSide, probeSide := right, left
// 	if len(left) < len(right) {
// 		buildSide, probeSide = left, right
// 	}

// 	// Build phase: create hash table
// 	hashTable := make(map[string][]Map)
// 	for _, item := range buildSide {
// 		buildRow, ok := item.(Map)
// 		if !ok {
// 			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", item))
// 		}

// 		// Extract join key(s) - now using side-specific extraction
// 		leftJoinKey := extractJoinKey(buildRow, joinExpr)
// 		hashTable[leftJoinKey] = append(hashTable[leftJoinKey], buildRow)
// 	}

// 	// Probe phase
// 	result := make([]any, 0)
// 	for _, item := range probeSide {
// 		probeRow, ok := item.(Map)
// 		if !ok {
// 			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", item))
// 		}

// 		rightJoinKey := extractJoinKey(probeRow, joinExpr)
// 		matchingRows, exists := hashTable[rightJoinKey]

// 		if exists {
// 			for _, matchingRow := range matchingRows {
// 				// Create joined row
// 				current := make(Map)
// 				for key, value := range probeRow {
// 					current[key] = value
// 				}
// 				for key, value := range matchingRow {
// 					current[key] = value
// 				}
// 				result = append(result, current)
// 			}
// 		}
// 	}

// 	return result, nil
// }

// SortedJoin - Sort both relations on join key, then merge
type SortableRow struct {
	Row     Map
	JoinKey string
}

// func ExecSortedJoin(query *Query, left []any, right []any, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
// 	if joinType == sqlparser.RightJoinType {
// 		left, right = right, left
// 	}

// 	// Convert and sort left relation
// 	leftSorted := make([]SortableRow, 0, len(left))
// 	for _, item := range left {
// 		row, ok := item.(Map)
// 		if !ok {
// 			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", item))
// 		}
// 		leftJoinKey := extractJoinKey(row, joinExpr)
// 		leftSorted = append(leftSorted, SortableRow{Row: row, JoinKey: leftJoinKey})
// 	}
// 	sort.Slice(leftSorted, func(i, j int) bool {
// 		return leftSorted[i].JoinKey < leftSorted[j].JoinKey
// 	})

// 	// Convert and sort right relation
// 	rightSorted := make([]SortableRow, 0, len(right))
// 	for _, item := range right {
// 		row, ok := item.(Map)
// 		if !ok {
// 			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", item))
// 		}
// 		rightJoinKey := extractJoinKey(row, joinExpr)
// 		rightSorted = append(rightSorted, SortableRow{Row: row, JoinKey: rightJoinKey})
// 	}
// 	sort.Slice(rightSorted, func(i, j int) bool {
// 		return rightSorted[i].JoinKey < rightSorted[j].JoinKey
// 	})

// 	// Merge join
// 	result := make([]any, 0)
// 	i, j := 0, 0

// 	for i < len(leftSorted) {
// 		leftRow := leftSorted[i]
// 		joined := false

// 		// Find all matching rows in right relation
// 		for j < len(rightSorted) && rightSorted[j].JoinKey < leftRow.JoinKey {
// 			j++
// 		}

// 		startJ := j
// 		for j < len(rightSorted) && rightSorted[j].JoinKey == leftRow.JoinKey {
// 			rightRow := rightSorted[j]

// 			// Create joined row
// 			current := make(Map)
// 			for key, value := range leftRow.Row {
// 				current[key] = value
// 			}
// 			for key, value := range rightRow.Row {
// 				current[key] = value
// 			}

// 			// Evaluate full join condition
// 			rs, err := Expr(query, current, joinExpr, nil)
// 			if err != nil {
// 				return nil, err
// 			}
// 			rsValue, ok := rs.(bool)
// 			if !ok {
// 				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", rs))
// 			}
// 			if rsValue {
// 				result = append(result, current)
// 				joined = true
// 			}
// 			j++
// 		}

// 		// Reset j for next left row with same key
// 		j = startJ

// 		// Handle left join case
// 		if !joined && joinType != sqlparser.NormalJoinType {
// 			current := make(Map)
// 			for key, value := range leftRow.Row {
// 				current[key] = value
// 			}
// 			result = append(result, current)
// 		}

// 		i++
// 	}

// 	return result, nil
// }

// // MergeJoin - Assumes both relations are already sorted on join key
// func ExecMergeJoin(query *Query, left []any, right []any, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
// 	if joinType == sqlparser.RightJoinType {
// 		left, right = right, left
// 	}

// 	result := make([]any, 0)
// 	i, j := 0, 0

// 	for i < len(left) {
// 		leftRow, ok := left[i].(Map)
// 		if !ok {
// 			return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", left[i]))
// 		}

// 		leftJoinKey := extractJoinKey(leftRow, joinExpr)
// 		joined := false

// 		// Skip right rows that are smaller than current left key
// 		for j < len(right) {
// 			rightRow, ok := right[j].(Map)
// 			if !ok {
// 				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", right[j]))
// 			}
// 			rightJoinKey := extractJoinKey(rightRow, joinExpr)
// 			if rightJoinKey < leftJoinKey {
// 				j++
// 			} else {
// 				break
// 			}
// 		}

// 		// Process all matching right rows
// 		startJ := j
// 		for j < len(right) {
// 			rightRow, ok := right[j].(Map)
// 			if !ok {
// 				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected object but found %T", right[j]))
// 			}
// 			rightJoinKey := extractJoinKey(rightRow, joinExpr)

// 			if rightJoinKey != leftJoinKey {
// 				break
// 			}

// 			// Create joined row
// 			current := make(Map)
// 			for key, value := range leftRow {
// 				current[key] = value
// 			}
// 			for key, value := range rightRow {
// 				current[key] = value
// 			}

// 			// Evaluate full join condition
// 			rs, err := Expr(query, current, joinExpr, nil)
// 			if err != nil {
// 				return nil, err
// 			}
// 			rsValue, ok := rs.(bool)
// 			if !ok {
// 				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", rs))
// 			}
// 			if rsValue {
// 				result = append(result, current)
// 				joined = true
// 			}
// 			j++
// 		}

// 		// Reset j for potential matches with next left row
// 		j = startJ

// 		// Handle left join case
// 		if !joined && joinType != sqlparser.NormalJoinType {
// 			current := make(Map)
// 			for key, value := range leftRow {
// 				current[key] = value
// 			}
// 			result = append(result, current)
// 		}

// 		i++
// 	}

// 	return result, nil
// }

// Helper function to extract left-side join key from row
func extractJoinKey(ident string, identRight string, joinExpr sqlparser.Expr) string {
	keys := extractJoinColumns(ident, identRight, joinExpr)
	_ = keys
	//return buildJoinKey(row, keys)
	return ""
}

// Build join key from specified columns
func buildJoinKey(row Map, keys []string) string {
	if len(keys) == 0 {
		// Fallback: if we can't parse the expression, create a composite key
		key := ""
		for k, v := range row {
			key += fmt.Sprintf("%s:%v;", k, v)
		}
		return key
	}

	// Create composite key from the specified columns
	keyParts := make([]string, 0, len(keys))
	for _, col := range keys {
		val, err := ExecReader(row, col)
		if err != nil {
			// Column doesn't exist in this row - this shouldn't happen if we parsed correctly
			keyParts = append(keyParts, "NULL")
			continue
		}
		if val != nil {
			keyParts = append(keyParts, fmt.Sprintf("%v", val))
		} else {
			keyParts = append(keyParts, "NULL")
		}
	}

	// Join with a delimiter that won't appear in normal data
	return fmt.Sprintf("[%s]", strings.Join(keyParts, "§"))
}

// Extract left-side column names from join expression
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

// Remove duplicate strings from slice
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

// Extract column names from a single expression
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

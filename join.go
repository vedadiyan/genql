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
	"time"

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

func ToHash(mapper [][]byte) (string, error) {
	var buffer bytes.Buffer
	for _, value := range mapper {
		// binary.Write(&buffer, binary.LittleEndian, value)
		// buffer.WriteString("-")
		buffer.Write(value)
		buffer.WriteString("-")
	}
	sha256 := sha256.New()
	_, err := sha256.Write(buffer.Bytes())
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
	for _, row := range rows {
		r := row
		hashMap := make([][]byte, 0)
		mapper := make(Map)
		for _, column := range columns {
			reader, err := ExecReader(row, column)
			if err != nil {
				return nil, err
			}
			binary.Write(&buffer, binary.LittleEndian, reader)
			hashMap = append(hashMap, buffer.Bytes())
			buffer.Reset()
			mapper[strings.ReplaceAll(column, "'", "")] = reader
		}
		hash, err := ToHash(hashMap)
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

// Original nested loop join (for reference)
func ExecJoin3(query *Query, left []any, right []any, leftIdent string, rightIdent string, into string, joinExpr sqlparser.Expr, joinType sqlparser.JoinType) ([]any, error) {
	if !joinType.IsLeftJoin() {
		left, right = right, left
	}

	l, err := ToCatalog(left, leftIdent, rightIdent, joinExpr)
	if err != nil {
		return nil, err
	}
	fmt.Println(len(l.Rows), "vs", len(left))
	then := time.Now()
	r, err := ToCatalog(right, rightIdent, leftIdent, joinExpr)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	fmt.Println(len(r.Rows), "vs", len(right))
	fmt.Println(now.Sub(then).Milliseconds())
	slice := make([]any, 0)
	if len(into) != 0 {
		for lk, lv := range l.Rows {
			rv, exist := r.Rows[lk]
			if exist || !joinType.IsInner() {
				mapper := make(Map)
				map2 := make(Map)
				if len(leftIdent) != 0 {
					left := make([]any, 0)
					for _, x := range lv {
						left = append(left, (*x).(Map)[leftIdent])
					}
					map2[leftIdent] = left
				} else {
					for i, x := range lv {
						if i > 0 {
							panic("")
						}
						maps.Copy(map2, (*x).(Map))
					}
				}
				if len(rightIdent) != 0 {
					right := make([]any, 0)
					for _, x := range rv {
						right = append(right, (*x).(Map)[rightIdent])
					}
					map2[rightIdent] = right
				} else {
					for i, x := range rv {
						if i > 0 {
							panic("")
						}
						maps.Copy(map2, (*x).(Map))
					}
				}
				maps.Copy(map2, *(l.Keys[lk]))
				maps.Copy(map2, *(r.Keys[lk]))
				mapper[into] = map2
				slice = append(slice, mapper)
			}
		}
	}
	return slice, nil
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

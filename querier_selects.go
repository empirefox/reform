package reform

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"gopkg.in/doug-martin/goqu.v3"
)

// NextRow scans next result row from rows to str. If str implements AfterFinder, it also calls AfterFind().
// It is caller's responsibility to call rows.Close().
//
// If there is no next result row, it returns ErrNoRows. It also may return rows.Next(), rows.Scan()
// and AfterFinder errors.
//
// See SelectRows example for idiomatic usage.
func (q *Querier) NextRow(str Struct, rows *sql.Rows) error {
	var err error
	next := rows.Next()
	if !next {
		err = rows.Err()
		if err == nil {
			err = ErrNoRows
		}
		return err
	}

	err = rows.Scan(str.Pointers()...)
	if err != nil {
		return err
	}

	if af, ok := str.(AfterFinder); ok {
		err = af.AfterFind()
	}
	return err
}

// selectQuery returns full SELECT query for given view and tail.
func (q *Querier) selectQuery(view View, tail string, limit1 bool) string {
	command := "SELECT"

	if limit1 && q.SelectLimitMethod() == SelectTop {
		command += " TOP 1"
	}

	return fmt.Sprintf("%s %s FROM %s %s",
		command, strings.Join(q.QualifiedColumns(view), ", "), q.QualifiedView(view), tail)
}

// SelectOneTo queries str's View with tail and args and scans first result to str.
// If str implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) SelectOneTo(str Struct, tail string, args ...interface{}) error {
	query := q.selectQuery(str.View(), tail, true)
	err := q.QueryRow(os.Expand(query, str.View().ToCol), args...).Scan(str.Pointers()...)
	if err != nil {
		return err
	}

	if af, ok := str.(AfterFinder); ok {
		err = af.AfterFind()
	}
	return err
}

func (q *Querier) DsSelectOneTo(str Struct, ds *goqu.Dataset) error {
	query, args, err := ds.From(str.View().Name()).Select(str.View().IColumns()...).Limit(1).ToSql()
	if err != nil {
		return err
	}

	err = q.QueryRow(os.Expand(query, str.View().ToCol), args...).Scan(str.Pointers()...)
	if err != nil {
		return err
	}

	if af, ok := str.(AfterFinder); ok {
		err = af.AfterFind()
	}
	return err
}

// SelectOneFrom queries view with tail and args and scans first result to new Struct str.
// If str implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns nil, ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) SelectOneFrom(view View, tail string, args ...interface{}) (Struct, error) {
	str := view.NewStruct()
	err := q.SelectOneTo(str, tail, args...)
	if err != nil {
		return nil, err
	}
	return str, nil
}

func (q *Querier) DsSelectOneFrom(view View, ds *goqu.Dataset) (Struct, error) {
	str := view.NewStruct()
	err := q.DsSelectOneTo(str, ds)
	if err != nil {
		return nil, err
	}
	return str, nil
}

// SelectRows queries view with tail and args and returns rows. They can then be iterated with NextRow().
// It is caller's responsibility to call rows.Close().
//
// In case of error rows will be nil. Error is never ErrNoRows.
//
// See example for idiomatic usage.
func (q *Querier) SelectRows(view View, tail string, args ...interface{}) (*sql.Rows, error) {
	query := q.selectQuery(view, tail, false)
	return q.Query(os.Expand(query, view.ToCol), args...)
}

func (q *Querier) DsSelectRows(view View, ds *goqu.Dataset) (*sql.Rows, error) {
	query, args, err := ds.From(view.Name()).Select(view.IColumns()...).ToSql()
	if err != nil {
		return nil, err
	}
	return q.Query(os.Expand(query, view.ToCol), args...)
}

func (q *Querier) DsCount(view View, ds *goqu.Dataset) (uint64, error) {
	query, args, err := ds.From(view.Name()).Select(goqu.COUNT(goqu.Star()).As("count")).ToSql()
	if err != nil {
		return 0, err
	}

	var count int64
	err = q.QueryRow(os.Expand(query, view.ToCol), args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	if count < 0 {
		count = 0
	}
	return uint64(count), nil
}

// SelectAllFrom queries view with tail and args and returns a slice of new Structs.
// If view's Struct implements AfterFinder, it also calls AfterFind().
//
// In case of query error slice will be nil. If error is encountered during iteration,
// partial result and error will be returned. Error is never ErrNoRows.
func (q *Querier) SelectAllFrom(view View, tail string, args ...interface{}) (structs []Struct, err error) {
	var rows *sql.Rows
	rows, err = q.SelectRows(view, tail, args...)
	if err != nil {
		return
	}
	defer func() {
		e := rows.Close()
		if err == nil {
			err = e
		}
	}()

	for {
		str := view.NewStruct()
		err = q.NextRow(str, rows)
		if err != nil {
			if err == ErrNoRows {
				err = nil
			}
			return
		}

		structs = append(structs, str)
	}
}

func (q *Querier) DsSelectAllFrom(view View, ds *goqu.Dataset) (structs []Struct, err error) {
	query, args, err := ds.From(view.Name()).Select(view.IColumns()...).ToSql()
	if err != nil {
		return
	}

	var rows *sql.Rows
	rows, err = q.Query(os.Expand(query, view.ToCol), args...)
	if err != nil {
		return
	}
	defer func() {
		e := rows.Close()
		if err == nil {
			err = e
		}
	}()

	for {
		str := view.NewStruct()
		err = q.NextRow(str, rows)
		if err != nil {
			if err == ErrNoRows {
				err = nil
			}
			return
		}

		structs = append(structs, str)
	}
}

// findTail returns a tail of SELECT query for given view, column and arg.
func (q *Querier) findTail(view string, column string, arg interface{}, limit1 bool) (tail string, needArg bool) {
	qi := q.QuoteIdentifier(view) + "." + q.QuoteIdentifier(column)
	if arg == nil {
		tail = fmt.Sprintf("WHERE %s IS NULL", qi)
	} else {
		tail = fmt.Sprintf("WHERE %s = %s", qi, q.Placeholder(1))
		needArg = true
	}

	if limit1 && q.SelectLimitMethod() == Limit {
		tail += " LIMIT 1"
	}

	return
}

// FindOneTo queries str's View with column and arg and scans first result to str.
// If str implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) FindOneTo(str Struct, column string, arg interface{}) error {
	tail, needArg := q.findTail(str.View().Name(), column, arg, true)
	if needArg {
		return q.SelectOneTo(str, tail, arg)
	}
	return q.SelectOneTo(str, tail)
}

func (q *Querier) DsFindOneTo(str Struct, ds *goqu.Dataset) error {
	return q.DsSelectOneTo(str, ds)
}

// FindOneFrom queries view with column and arg and scans first result to new Struct str.
// If str implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns nil, ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) FindOneFrom(view View, column string, arg interface{}) (Struct, error) {
	tail, needArg := q.findTail(view.Name(), column, arg, true)
	if needArg {
		return q.SelectOneFrom(view, tail, arg)
	}
	return q.SelectOneFrom(view, tail)
}

func (q *Querier) DsFindOneFrom(view View, ds *goqu.Dataset) (Struct, error) {
	return q.DsSelectOneFrom(view, ds)
}

// FindRows queries view with column and arg and returns rows. They can then be iterated with NextRow().
// It is caller's responsibility to call rows.Close().
//
// In case of error rows will be nil. Error is never ErrNoRows.
//
// See SelectRows example for idiomatic usage.
func (q *Querier) FindRows(view View, column string, arg interface{}) (*sql.Rows, error) {
	tail, needArg := q.findTail(view.Name(), column, arg, false)
	if needArg {
		return q.SelectRows(view, tail, arg)
	}
	return q.SelectRows(view, tail)
}

func (q *Querier) DsFindRows(view View, ds *goqu.Dataset) (*sql.Rows, error) {
	return q.DsSelectRows(view, ds)
}

// FindAllFrom queries view with column and args and returns a slice of new Structs.
// If view's Struct implements AfterFinder, it also calls AfterFind().
//
// In case of query error slice will be nil. If error is encountered during iteration,
// partial result and error will be returned. Error is never ErrNoRows.
func (q *Querier) FindAllFrom(view View, column string, args ...interface{}) ([]Struct, error) {
	p := strings.Join(q.Placeholders(1, len(args)), ", ")
	qi := q.QualifiedView(view) + "." + q.QuoteIdentifier(column)
	tail := fmt.Sprintf("WHERE %s IN (%s)", qi, p)
	return q.SelectAllFrom(view, tail, args...)
}

func (q *Querier) FindAllFromPK(table Table, args ...interface{}) ([]Struct, error) {
	if len(args) == 0 {
		return nil, ErrNoPK
	}
	p := strings.Join(q.Placeholders(1, len(args)), ", ")
	qi := q.QualifiedView(table) + "." + q.QuoteIdentifier(table.PK())
	tail := fmt.Sprintf("WHERE %s IN (%s)", qi, p)
	return q.SelectAllFrom(table, tail, args...)
}

func (q *Querier) DsFindAllFrom(view View, ds *goqu.Dataset) ([]Struct, error) {
	return q.DsSelectAllFrom(view, ds)
}

// FindByPrimaryKeyTo queries record's Table with primary key and scans first result to record.
// If record implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) FindByPrimaryKeyTo(record Record, pk interface{}) error {
	table := record.Table()
	return q.FindOneTo(record, table.Columns()[table.PKColumnIndex()], pk)
}

// FindByPrimaryKeyFrom queries table with primary key and scans first result to new Record.
// If record implements AfterFinder, it also calls AfterFind().
//
// If there are no rows in result, it returns nil, ErrNoRows. It also may return QueryRow(), Scan()
// and AfterFinder errors.
func (q *Querier) FindByPrimaryKeyFrom(table Table, pk interface{}) (Record, error) {
	record := table.NewRecord()
	err := q.FindOneTo(record, table.Columns()[table.PKColumnIndex()], pk)
	if err != nil {
		return nil, err
	}
	return record, nil
}

// Reload is a shortcut for FindByPrimaryKeyTo for given record.
func (q *Querier) Reload(record Record) error {
	return q.FindByPrimaryKeyTo(record, record.PKValue())
}

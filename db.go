package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"regexp"
)

func MapToSlice(query string, mp interface{}) (string, []interface{}, error) {
	vv := reflect.ValueOf(mp)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return "", []interface{}{}, ErrNoMapPointer
	}

	args := make([]interface{}, 0, len(vv.Elem().MapKeys()))
	var err error
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		v := vv.Elem().MapIndex(reflect.ValueOf(src[1:]))
		if !v.IsValid() {
			err = fmt.Errorf("map key %s is missing", src[1:])
		} else {
			args = append(args, v.Interface())
		}
		return "?"
	})

	return query, args, err
}

func StructToSlice(query string, st interface{}) (string, []interface{}, error) {
	vv := reflect.ValueOf(st)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
		return "", []interface{}{}, ErrNoStructPointer
	}

	args := make([]interface{}, 0)
	var err error
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		fv := vv.Elem().FieldByName(src[1:]).Interface()
		if v, ok := fv.(driver.Valuer); ok {
			var value driver.Value
			value, err = v.Value()
			if err != nil {
				return "?"
			}
			args = append(args, value)
		} else {
			args = append(args, fv)
		}
		return "?"
	})
	if err != nil {
		return "", []interface{}{}, err
	}
	return query, args, nil
}

type DB struct {
	*sql.DB
	Mapper IMapper
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{db, NewCacheMapper(&SnakeMapper{})}, nil
}

func FromDB(db *sql.DB) *DB {
	return &DB{db, NewCacheMapper(&SnakeMapper{})}
}

func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	rows, err := db.DB.QueryContext(ctx, query, args...)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}
	return &Rows{rows, db.Mapper}, nil
}

func (db *DB) QueryMap(query string, mp interface{}) (*Rows, error) {
	return db.QueryMapContext(context.Background(), query, mp)
}

func (db *DB) QueryMapContext(ctx context.Context, query string, mp interface{}) (*Rows, error) {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, query, args...)
}

func (db *DB) QueryStruct(query string, st interface{}) (*Rows, error) {
	return db.QueryStructContext(context.Background(), query, st)
}

func (db *DB) QueryStructContext(ctx context.Context, query string, st interface{}) (*Rows, error) {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &Row{nil, err}
	}
	return &Row{rows, nil}
}

func (db *DB) QueryRowMap(query string, mp interface{}) *Row {
	return db.QueryRowMapContext(context.Background(), query, mp)
}

func (db *DB) QueryRowMapContext(ctx context.Context, query string, mp interface{}) *Row {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return &Row{nil, err}
	}
	return db.QueryRowContext(ctx, query, args...)
}

func (db *DB) QueryRowStruct(query string, st interface{}) *Row {
	return db.QueryRowStructContext(context.Background(), query, st)
}

func (db *DB) QueryRowStructContext(ctx context.Context, query string, st interface{}) *Row {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return &Row{nil, err}
	}
	return db.QueryRowContext(ctx, query, args...)
}

type Stmt struct {
	*sql.Stmt
	Mapper IMapper
	names  map[string]int
}

func (db *DB) Prepare(query string) (*Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	names := make(map[string]int)
	var i int
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		names[src[1:]] = i
		i += 1
		return "?"
	})

	stmt, err := db.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt, db.Mapper, names}, nil
}

func (s *Stmt) ExecMap(mp interface{}) (sql.Result, error) {
	return s.ExecMapContext(context.Background(), mp)
}

func (s *Stmt) ExecMapContext(ctx context.Context, mp interface{}) (sql.Result, error) {
	vv := reflect.ValueOf(mp)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return nil, errors.New("mp should be a map's pointer")
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().MapIndex(reflect.ValueOf(k)).Interface()
	}
	return s.Stmt.ExecContext(ctx, args...)
}

func (s *Stmt) ExecStruct(st interface{}) (sql.Result, error) {
	return s.ExecStructContext(context.Background(), st)
}

func (s *Stmt) ExecStructContext(ctx context.Context, st interface{}) (sql.Result, error) {
	vv := reflect.ValueOf(st)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
		return nil, errors.New("mp should be a map's pointer")
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().FieldByName(k).Interface()
	}
	return s.Stmt.ExecContext(ctx, args...)
}

func (s *Stmt) Query(args ...interface{}) (*Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*Rows, error) {
	rows, err := s.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows, s.Mapper}, nil
}

func (s *Stmt) QueryMap(mp interface{}) (*Rows, error) {
	return s.QueryMapContext(context.Background(), mp)
}

func (s *Stmt) QueryMapContext(ctx context.Context, mp interface{}) (*Rows, error) {
	vv := reflect.ValueOf(mp)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return nil, errors.New("mp should be a map's pointer")
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().MapIndex(reflect.ValueOf(k)).Interface()
	}

	return s.QueryContext(ctx, args...)
}

func (s *Stmt) QueryStruct(st interface{}) (*Rows, error) {
	return s.QueryStructContext(context.Background(), st)
}

func (s *Stmt) QueryStructContext(ctx context.Context, st interface{}) (*Rows, error) {
	vv := reflect.ValueOf(st)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
		return nil, errors.New("mp should be a map's pointer")
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().FieldByName(k).Interface()
	}
	return s.QueryContext(ctx, args...)
}

func (s *Stmt) QueryRow(args ...interface{}) *Row {
	return s.QueryRowContext(context.Background(), args...)
}

func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *Row {
	rows, err := s.QueryContext(ctx, args...)
	return &Row{rows, err}
}

func (s *Stmt) QueryRowMap(mp interface{}) *Row {
	return s.QueryRowMapContext(context.Background(), mp)
}

func (s *Stmt) QueryRowMapContext(ctx context.Context, mp interface{}) *Row {
	vv := reflect.ValueOf(mp)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return &Row{nil, errors.New("mp should be a map's pointer")}
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().MapIndex(reflect.ValueOf(k)).Interface()
	}

	return s.QueryRowContext(ctx, args...)
}

func (s *Stmt) QueryRowStruct(st interface{}) *Row {
	return s.QueryRowStructContext(context.Background(), st)
}

func (s *Stmt) QueryRowStructContext(ctx context.Context, st interface{}) *Row {
	vv := reflect.ValueOf(st)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
		return &Row{nil, errors.New("st should be a struct's pointer")}
	}

	args := make([]interface{}, len(s.names))
	for k, i := range s.names {
		args[i] = vv.Elem().FieldByName(k).Interface()
	}

	return s.QueryRowContext(ctx, args...)
}

var (
	re = regexp.MustCompile(`[?](\w+)`)
)

// insert into (name) values (?)
// insert into (name) values (?name)
func (db *DB) ExecMap(query string, mp interface{}) (sql.Result, error) {
	return db.ExecMapContext(context.Background(), query, mp)
}

func (db *DB) ExecMapContext(ctx context.Context, query string, mp interface{}) (sql.Result, error) {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return nil, err
	}
	return db.DB.ExecContext(ctx, query, args...)
}

func (db *DB) ExecStruct(query string, st interface{}) (sql.Result, error) {
	return db.ExecStructContext(context.Background(), query, st)
}

func (db *DB) ExecStructContext(ctx context.Context, query string, st interface{}) (sql.Result, error) {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return nil, err
	}
	return db.DB.ExecContext(ctx, query, args...)
}

type EmptyScanner struct {
}

func (EmptyScanner) Scan(src interface{}) error {
	return nil
}

type Tx struct {
	*sql.Tx
	Mapper IMapper
}

func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx, db.Mapper}, nil
}

func (tx *Tx) Prepare(query string) (*Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	names := make(map[string]int)
	var i int
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		names[src[1:]] = i
		i += 1
		return "?"
	})

	stmt, err := tx.Tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt, tx.Mapper, names}, nil
}

func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	// TODO:
	return tx.StmtContext(context.Background(), stmt)
}

func (tx *Tx) StmtContext(ctx context.Context, stmt *Stmt) *Stmt {
	// TODO:
	return stmt
}

func (tx *Tx) ExecMap(query string, mp interface{}) (sql.Result, error) {
	return tx.ExecMapContext(context.Background(), query, mp)
}

func (tx *Tx) ExecMapContext(ctx context.Context, query string, mp interface{}) (sql.Result, error) {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) ExecStruct(query string, st interface{}) (sql.Result, error) {
	return tx.ExecStructContext(context.Background(), query, st)
}

func (tx *Tx) ExecStructContext(ctx context.Context, query string, st interface{}) (sql.Result, error) {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows, tx.Mapper}, nil
}

func (tx *Tx) QueryMap(query string, mp interface{}) (*Rows, error) {
	return tx.QueryMapContext(context.Background(), query, mp)
}

func (tx *Tx) QueryMapContext(ctx context.Context, query string, mp interface{}) (*Rows, error) {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return nil, err
	}
	return tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryStruct(query string, st interface{}) (*Rows, error) {
	return tx.QueryStructContext(context.Background(), query, st)
}

func (tx *Tx) QueryStructContext(ctx context.Context, query string, st interface{}) (*Rows, error) {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return nil, err
	}
	return tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := tx.QueryContext(ctx, query, args...)
	return &Row{rows, err}
}

func (tx *Tx) QueryRowMap(query string, mp interface{}) *Row {
	return tx.QueryRowMapContext(context.Background(), query, mp)
}

func (tx *Tx) QueryRowMapContext(ctx context.Context, query string, mp interface{}) *Row {
	query, args, err := MapToSlice(query, mp)
	if err != nil {
		return &Row{nil, err}
	}
	return tx.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) QueryRowStruct(query string, st interface{}) *Row {
	return tx.QueryRowStructContext(context.Background(), query, st)
}

func (tx *Tx) QueryRowStructContext(ctx context.Context, query string, st interface{}) *Row {
	query, args, err := StructToSlice(query, st)
	if err != nil {
		return &Row{nil, err}
	}
	return tx.QueryRowContext(ctx, query, args...)
}

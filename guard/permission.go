package guard

import (
	"fmt"
	"reflect"

	"gorm.io/gorm/schema"
)

type writeOp int

const (
	opCreate writeOp = iota
	opUpdate
)

// taggedWritable interprets the gorm permission tags for guard's purposes:
// `gorm:"->"` is read-only, `gorm:"->:false"` is write-only. A field tagged
// `gorm:"-"` is neither readable nor writable. GORM itself maps `->:false` to
// Creatable=false, which does not distinguish write-only from read-only, so
// the raw tag is consulted here.
func taggedWritable(f *schema.Field, op writeOp) bool {
	if v, ok := f.TagSettings["-"]; ok && (v == "-" || v == "all") {
		return false
	}
	if v, ok := f.TagSettings["->"]; ok {
		if v == "false" {
			// write-only: accepted by create and update
			return true
		}
		return false // read-only
	}
	if op == opCreate {
		return f.Creatable
	}
	return f.Updatable
}

// taggedReadable reports whether the tag allows the column in exports.
func taggedReadable(f *schema.Field) bool {
	if v, ok := f.TagSettings["-"]; ok && (v == "-" || v == "all") {
		return false
	}
	if v, ok := f.TagSettings["->"]; ok {
		return v != "false"
	}
	return f.Readable
}

// checkWrite is the single permission gate used by single-row and batch
// creates and by every update form. value may be a struct, a pointer, a slice
// of either, or a map[string]interface{} used with an explicit table.
func (s *Store) checkWrite(sess *Session, table string, value interface{}, op writeOp) (string, error) {
	if value == nil {
		return table, nil
	}
	if m, ok := value.(map[string]interface{}); ok {
		return table, s.checkMap(sess, table, m, op)
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return table, nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := 0; i < rv.Len(); i++ {
			if _, err := s.checkWrite(sess, table, rv.Index(i).Interface(), op); err != nil {
				return table, err
			}
		}
		return table, nil
	}
	if rv.Kind() != reflect.Struct {
		return table, nil
	}
	sch, err := s.parseSchema(rv.Interface())
	if err != nil || sch == nil {
		return table, err
	}
	if table == "" {
		table = sch.Table
	}
	if err := s.checkStructFields(sess, table, sch, rv, op, map[reflect.Type]bool{}); err != nil {
		return table, err
	}
	return table, nil
}

func (s *Store) checkStructFields(sess *Session, table string, sch *schema.Schema, rv reflect.Value, op writeOp, seen map[reflect.Type]bool) error {
	for _, f := range sch.Fields {
		writable := taggedWritable(f, op)
		if f.PrimaryKey {
			continue
		}
		fv, zero := f.ValueOf(nil, rv)
		if zero {
			continue
		}
		if !s.canWrite(sess, table, f.DBName, writable) {
			s.auditor.trace(EventDenied,
				fmt.Sprintf("table=%s column=%s op=%d value=%v", table, f.DBName, op, fv), "", nil)
			return fmt.Errorf("%w: %s.%s (value=%v)", ErrReadOnlyField, table, f.DBName, fv)
		}
	}
	// Nested association writes go through the same gate.
	if seen[rv.Type()] {
		return nil
	}
	seen[rv.Type()] = true
	for _, rel := range sch.Relationships.Relations {
		rf := rv.FieldByName(rel.Field.Name)
		if !rf.IsValid() {
			continue
		}
		fv := rf
		for fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				fv = reflect.Value{}
				break
			}
			fv = fv.Elem()
		}
		if !fv.IsValid() {
			continue
		}
		switch fv.Kind() {
		case reflect.Struct:
			if isZero(fv) {
				continue
			}
			if _, err := s.checkWrite(sess, rel.FieldSchema.Table, fv.Addr().Interface(), op); err != nil {
				return err
			}
		case reflect.Slice, reflect.Array:
			if fv.Len() == 0 {
				continue
			}
			if _, err := s.checkWrite(sess, rel.FieldSchema.Table, fv.Interface(), op); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) checkMap(sess *Session, table string, m map[string]interface{}, op writeOp) error {
	prototype, err := s.tablePrototype(table)
	if err != nil || prototype == nil {
		return err
	}
	sch, err := s.parseSchema(prototype)
	if err != nil || sch == nil {
		return err
	}
	for key, val := range m {
		f := sch.LookUpField(key)
		if f == nil {
			continue
		}
		writable := taggedWritable(f, op)
		if writable && f.PrimaryKey {
			continue
		}
		if reflect.DeepEqual(val, reflect.Zero(f.FieldType).Interface()) {
			continue
		}
		if !s.canWrite(sess, table, f.DBName, writable) {
			s.auditor.trace(EventDenied,
				fmt.Sprintf("table=%s column=%s op=%d value=%v", table, f.DBName, op, val), "", nil)
			return fmt.Errorf("%w: %s.%s (value=%v)", ErrReadOnlyField, table, f.DBName, val)
		}
	}
	return nil
}

func (s *Store) tablePrototype(table string) (interface{}, error) {
	if prototype, ok := s.schemaCache.Load(table); ok {
		if sch, ok := prototype.(*schema.Schema); ok {
			return reflect.New(sch.ModelType).Interface(), nil
		}
	}
	return nil, nil
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !v.Field(i).IsZero() {
				return false
			}
		}
		return true
	default:
		return v.IsZero()
	}
}

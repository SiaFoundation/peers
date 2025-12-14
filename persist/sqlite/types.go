package sqlite

import (
	"database/sql/driver"
	"errors"
	"time"
)

type (
	sqlTime time.Time
)

func (st sqlTime) Value() (driver.Value, error) {
	return time.Time(st).UnixMilli(), nil
}

func (st *sqlTime) Scan(src any) error {
	if t, ok := src.(int64); ok {
		*st = sqlTime(time.UnixMilli(t))
		return nil
	}
	return errors.New("invalid type")
}

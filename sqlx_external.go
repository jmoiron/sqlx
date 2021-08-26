package sqlx

import "database/sql"

func RowsFromExternal(r *sql.Rows, db *DB) *Rows {
	return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}
}

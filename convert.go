package sqlx

import (
	_ "unsafe"
)

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src interface{}) error

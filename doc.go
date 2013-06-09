// General purpose extensions to database/sql
//
// sqlx is intended to seamlessly wrap database/sql and provide some convenience
// methods which range from basic common error handling techniques to complex
// reflect-base Scan extensions.  Replacing `sql.Open` with `sqlx.Open` should
// provide access to most of the features within sqlx while not changing the
// interface used by any existing code.
//
// sqlx introduces the following concepts which are accessible wherever they
// make sense:
//
// The addition of a mnemonic set of "Exec" functions:
//
//	Execv - log.Println errors, return (rows, err)
//	Execl - log.Println errors, return only rows
//	Execp - panic(err) on error
//	Execf - log.Fatal(err) on error
// 	MustExec - same as Execp
//
// The addition of a "StructScan" function, which takes an the result from a
// query and a struct slice and automatically scans each row as a struct.
//
// The addition of a set of "Select" functions, which combine Query and
// StructScan and have "f" and "v" error handling variantes like Exec.
//
// The addition of a "Get" function, which is to "QueryRow" what "Select" is
// to "Query", and will return a special Row that can StructScan.
//
// The addition of Named Queries, accessible via either struct arguments or
// via map arguments
//
// A "LoadFile" convenience function which executes the queries in a file.
//
// A "Connect" function, which combines "Open" and "Ping", and panicing variants
// of Connect and Begin: MustConnect, MustBegin.
//
package sqlx

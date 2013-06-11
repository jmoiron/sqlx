// General purpose extensions to database/sql
//
// sqlx is intended to seamlessly wrap database/sql and provide convenience
// methods which are useful in the development of database driven applications.
// None of the underlying database/sql methods are changed, instead all extended
// behavior is implemented through new methods defined on wrapper types.
//
// sqlx adds struct scanning, named queries, query rebinding between drivers,
// convenient shorthand for common error handling, from-file query execution,
// and more.
//
package sqlx

// Package schema holds the Control Plane SQL schema (§8), embedded so tests and
// tooling can apply it without file I/O.
package schema

import _ "embed"

// SQL is the full DDL for the Control Plane metadata database.
//
//go:embed schema.sql
var SQL string

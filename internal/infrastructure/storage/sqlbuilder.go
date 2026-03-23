package storage

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type sqlBuilder struct {
	where   []string
	orderBy []string
	args    pgx.NamedArgs
}

func newSQLBuilder() *sqlBuilder {
	return &sqlBuilder{
		where:   []string{},
		orderBy: []string{},
		args:    pgx.NamedArgs{},
	}
}

func (b *sqlBuilder) Where(expr string) {
	if expr == "" {
		return
	}
	b.where = append(b.where, expr)
}

func (b *sqlBuilder) OrderBy(expr string) {
	if expr == "" {
		return
	}
	b.orderBy = append(b.orderBy, expr)
}

func (b *sqlBuilder) Bind(name string, value any) string {
	b.args[name] = value
	return fmt.Sprintf("@%s", name)
}

func (b *sqlBuilder) WhereClause() string {
	if len(b.where) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(b.where, " AND ")
}

func (b *sqlBuilder) OrderByClause() string {
	if len(b.orderBy) == 0 {
		return ""
	}
	return "ORDER BY " + strings.Join(b.orderBy, ", ")
}

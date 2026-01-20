package service

import (
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

package db

import "gorm.io/gorm"

func EnsureSchema(d *gorm.DB, schema string) error {
	return d.Exec(`CREATE SCHEMA IF NOT EXISTS "` + schema + `"`).Error
}

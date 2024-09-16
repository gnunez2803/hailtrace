package weather

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlRepository struct {
	DB *sql.DB
}

func NewMysqlRepository(connString string) (*MysqlRepository, error) {
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Minute * 3)
	return &MysqlRepository{DB: db}, nil
}

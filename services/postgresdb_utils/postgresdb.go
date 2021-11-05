package postgresdb_utils

import (
	"database/sql"
	"fmt"
)

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "098poiA#"
	DB_NAME     = "things"
	HOST = "10.16.150.132"
	PORT = "5432"
)
func SetupDB() (*sql.DB,error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=%s", DB_USER, DB_PASSWORD, DB_NAME,HOST)
	db, err := sql.Open("postgres", dbinfo)
	if err!=nil{
		return nil,err
	}
	return db,err
}

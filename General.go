package mysqlmetaquery

import (
	"database/sql"

	"github.com/guinso/rdbmstool"
)

//IsDbTableExists check db table is exists or not
//db is database handler
//dbName is targeted database name; sometimes it is called as database schema
//tableName is targeted data table name
func IsDbTableExists(db rdbmstool.DbHandlerProxy, dbName string, tableName string) (bool, error) {
	SQLStr := "SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"

	row := db.QueryRow(SQLStr, dbName, tableName)
	var tmpCnt int
	err := row.Scan(&tmpCnt)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, err
	}

	return tmpCnt == 1, nil
}

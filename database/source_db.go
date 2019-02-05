package database

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kafitz/itinerum-coordinates-csv-dumper/models"
)

var db *sqlx.DB

var dbConfig = models.DatabaseConfig{
	Host:     "localhost",
	Port:     5432,
	Database: "dbname",
	User:     "user",
	Password: "pass"}

// ConnectSourceDB opens the connection to the source Itinerum database in Postgres
func ConnectSourceDB() {
	log.Println("Connecting to database...")
	var err error
	connectParams := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		dbConfig.User, dbConfig.Password, dbConfig.Database, dbConfig.Host, dbConfig.Port)
	db, err = sqlx.Open("postgres", connectParams)
	if err != nil {
		log.Fatalf("Could not connect to database -- %+v\n", err)
	}
}

// GetSurveyID returns the database ID integer for a given survey by name
func GetSurveyID(name string) (surveyID int, err error) {
	stmt, err := db.Prepare(`SELECT id FROM surveys WHERE name=$1`)
	if err != nil {
		return surveyID, err
	}
	err = stmt.QueryRow(name).Scan(&surveyID)
	return surveyID, err
}

// FetchCoordinateTimestampRange determines the relevant first and last row coordinate row IDs for a given survey
func FetchCoordinateTimestampRange(surveyID int) (firstCoordinatesTime time.Time, lastCoordinatesTime time.Time) {
	err := db.QueryRow(`SELECT timestamp
						FROM mobile_coordinates
						WHERE survey_id=$1
						AND timestamp IS NOT NULL
						AND timestamp > '2017-01-01 00:00:00'
						ORDER BY timestamp ASC
						LIMIT 1`, surveyID).Scan(&firstCoordinatesTime)
	if err != nil {
		log.Fatalln(err)
	}
	err = db.QueryRow(`SELECT timestamp
					   FROM mobile_coordinates
					   WHERE survey_id=$1
					   AND timestamp IS NOT NULL
					   ORDER BY timestamp DESC
					   LIMIT 1`, surveyID).Scan(&lastCoordinatesTime)
	if err != nil {
		log.Fatalln(err)
	}
	return firstCoordinatesTime, lastCoordinatesTime
}

// PrepareCoordinatesQuery generates a coordinate for iterating over the coordinates table
func PrepareCoordinatesQuery() *sqlx.Stmt {
	query := `
		SELECT *
		FROM mobile_coordinates
		WHERE survey_id=$1
		AND timestamp > $2
		ORDER BY timestamp ASC
		LIMIT 500000
	`
	stmt, err := db.Preparex(query)
	if err != nil {
		log.Fatalln(err)
	}
	return stmt
}

// PopulateUUIDLookup creates a database `mobile_id` -> `uuid` map to so that a JOIN on the `coordinates` table is avoided
func PopulateUUIDLookup(surveyID int) map[int64]string {
	var uuidLookup = make(map[int64]string)

	query := `SELECT id, uuid FROM mobile_users WHERE survey_id=$1`
	rows, err := db.Query(query, surveyID)
	if err != nil {
		log.Fatalln(err)
	}
	for rows.Next() {
		var mobileID int64
		var uuid string
		rows.Scan(&mobileID, &uuid)
		uuidLookup[mobileID] = uuid
	}
	return uuidLookup
}

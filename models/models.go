package models

import (
	"database/sql"
	"strconv"

	"github.com/lib/pq"
)

// DatabaseConfig provides a model for connecting to database
type DatabaseConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// Coordinate is the model for coordinates as represented in the database
type Coordinate struct {
	ID            int64
	SurveyID      int64 `db:"survey_id"`
	MobileID      int64 `db:"mobile_id"`
	MobileUUID    string
	Latitude      float64
	Longitude     float64
	Altitude      sql.NullFloat64
	Speed         float64
	Direction     sql.NullFloat64
	HAccuracy     float64       `db:"h_accuracy"`
	VAccuracy     float64       `db:"v_accuracy"`
	AccelerationX float64       `db:"acceleration_x"`
	AccelerationY float64       `db:"acceleration_y"`
	AccelerationZ float64       `db:"acceleration_z"`
	ModeDetected  sql.NullInt64 `db:"mode_detected"`
	PointType     sql.NullInt64 `db:"point_type"`
	Timestamp     pq.NullTime
}

// CSVHeaders returns the struct fields formatted as a .csv row
func (c Coordinate) CSVHeaders() []string {
	return []string{"uuid", "latitude", "longitude", "altitude", "speed", "direction",
		"h_accuracy", "v_accuracy", "acceleration_x", "acceleration_y", "acceleration_z",
		"mode_detected", "point_type", "timestamp"}
}

// CSVValues returns the struct values formatted as a .csv row
func (c Coordinate) CSVValues() (values []string) {
	values = []string{
		c.MobileUUID,
		strconv.FormatFloat(c.Latitude, 'f', -1, 64),
		strconv.FormatFloat(c.Longitude, 'f', -1, 64),
		floatValueOrBlank(c.Altitude),
		strconv.FormatFloat(c.Speed, 'f', -1, 64),
		floatValueOrBlank(c.Direction),
		strconv.FormatFloat(c.HAccuracy, 'f', -1, 64),
		strconv.FormatFloat(c.VAccuracy, 'f', -1, 64),
		strconv.FormatFloat(c.AccelerationX, 'f', -1, 64),
		strconv.FormatFloat(c.AccelerationY, 'f', -1, 64),
		strconv.FormatFloat(c.AccelerationZ, 'f', -1, 64),
		intValueOrBlank(c.ModeDetected),
		intValueOrBlank(c.PointType),
		timestampValueOrBlank(c.Timestamp)}
	return values
}

func floatValueOrBlank(value sql.NullFloat64) (stringValue string) {
	if value.Valid {
		stringValue = strconv.FormatFloat(value.Float64, 'f', -1, 64)
	} else {
		stringValue = ""
	}
	return stringValue
}

func intValueOrBlank(value sql.NullInt64) (stringValue string) {
	if value.Valid {
		stringValue = strconv.FormatInt(value.Int64, 10)
	} else {
		stringValue = ""
	}
	return stringValue
}

func timestampValueOrBlank(value pq.NullTime) (stringValue string) {
	if value.Valid {
		stringValue = value.Time.Format("2006-01-02T15:04:05")
	} else {
		stringValue = ""
	}
	return stringValue
}

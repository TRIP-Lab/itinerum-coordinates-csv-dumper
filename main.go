package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kafitz/itinerum-coordinates-csv-dumper/database"
	"github.com/kafitz/itinerum-coordinates-csv-dumper/fileio"
	"github.com/kafitz/itinerum-coordinates-csv-dumper/models"
	"github.com/lib/pq"
)

type seenItem struct {
	MobileID  int64
	Timestamp pq.NullTime
	Latitude  float64
	Longitude float64
}

// checkIfDuplicate returns true or false depending whether two rows share identical information
func checkIfDuplicate(prev models.Coordinate, c models.Coordinate) bool {
	return (prev.MobileID == c.MobileID &&
		prev.Timestamp == c.Timestamp &&
		prev.Latitude == c.Latitude &&
		prev.Longitude == c.Longitude)
}

// createUniqueRecord returns a Coordinates struct if a given database row is unique or returns an error indicating is is a "duplicate record"
func createUniqueRecord(c *models.Coordinate, seen *map[seenItem]bool, uuidLookup *map[int64]string) (err error) {
	// fetch the timestamp of any item in last group of rows if one exists
	var lastTimestamp pq.NullTime
	if len(*seen) > 0 {
		for k := range *seen {
			lastTimestamp = k.Timestamp
			break
		}
	}

	// read database row to duplicates struct object
	c.MobileUUID = (*uuidLookup)[c.MobileID]

	record := seenItem{
		MobileID:  c.MobileID,
		Timestamp: c.Timestamp,
		Latitude:  c.Latitude,
		Longitude: c.Longitude}

	// check whether duplicate record at this timestamp already exists; if not,
	// initialize a new map to limit memory usage
	if c.Timestamp != lastTimestamp {
		*seen = make(map[seenItem]bool)
	} else {
		if _, ok := (*seen)[record]; ok {
			err = errors.New("duplicate record")
		}
	}
	(*seen)[record] = true
	if err != nil {
		return err
	}

	// filter records where timestamp is a zero-value
	nullTime := time.Time{}
	if c.Timestamp.Time == nullTime {
		err = errors.New("null timestamp")
	}

	return err
}

// queryLooper iteratates through the coordinates table in chunks allowing the data to be written to multiple sources on a single-pass read of the dataset
func queryLooper(surveyID int, surveyName string, uuidLookup *map[int64]string) {
	log.Println("Querying first and last coordinate ID for survey...")
	firstCoordinatesTime, lastCoordinatesTime := database.FetchCoordinateTimestampRange(surveyID)
	fmt.Printf("First coordinate: %s / Last coordinate: %s\n", firstCoordinatesTime, lastCoordinatesTime)

	// function variables
	totalDuplicates := 0
	totalProcessed := 0
	offsetTime := firstCoordinatesTime
	headersWritten := false
	csvWriter, csvFile := fileio.OpenCSVWriter(surveyName)
	stmt := database.PrepareCoordinatesQuery()

	seenRows := make(map[seenItem]bool)
	for offsetTime.Before(lastCoordinatesTime) {
		log.Printf("Fetching coordinates since: %s\n", offsetTime)

		// execute batch database query
		rows, err := stmt.Queryx(surveyID, offsetTime)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		// process rows in chunks from database
		for rows.Next() {
			c := models.Coordinate{}
			err = rows.StructScan(&c)
			if err != nil {
				log.Fatalln(err)
			}

			err = createUniqueRecord(&c, &seenRows, uuidLookup)
			if err != nil {
				if err.Error() == "duplicate record" {
					totalDuplicates++
					continue
				} else if err.Error() == "null timestamp" {
					continue
				} else {
					log.Fatalf("Could not scan row: %+v\n", err)
				}
			}
			offsetTime = c.Timestamp.Time

			fileio.WriteCoordinateCSV(csvWriter, &headersWritten, &c)
			totalProcessed++
		}
		csvWriter.Flush()

		defer rows.Close()
		// output debug results
		pct := float32(totalDuplicates) / float32(totalProcessed) * 100.
		fmt.Printf("Total dupes: %d / Total processed: %d / Dupe rate: %2.2f%%\n", totalDuplicates, totalProcessed, pct)
	}

	fileio.CloseCSVWriter(csvWriter, csvFile)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalln("Inadequate number of commandline arguments, please provide a survey name.")
	}
	surveyName := os.Args[1]
	database.ConnectSourceDB()

	log.Printf("Fetching survey ID for %s...\n", surveyName)
	surveyID, err := database.GetSurveyID(surveyName)
	if err != nil {
		log.Fatalf("Error fetching survey ID for %s -- %+v\n", surveyName, err)
	}

	log.Println("Creating mobile ID -> uuid lookup...")
	uuidLookup := database.PopulateUUIDLookup(surveyID)

	log.Println("Dumping coordinates from database to .csv...")
	queryLooper(surveyID, surveyName, &uuidLookup)

	fmt.Println("Coordinates export finished.")
}

package fileio

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/kafitz/itinerum-coordinates-csv-dumper/models"
)

// OpenCSVWriter creates a filehandler to a .csv output file
func OpenCSVWriter(surveyName string) (writer *csv.Writer, file *os.File) {
	filename := fmt.Sprintf("%s-coordinates_filtered.csv", surveyName)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Cannot create file", err)
	}

	writer = csv.NewWriter(file)
	return writer, file
}

// CloseCSVWriter cleans up CSV file connection
func CloseCSVWriter(csvWriter *csv.Writer, csvFile *os.File) {
	csvWriter.Flush()
	csvFile.Close()
}

// WriteCoordinateCSV writes a chunk of rows to an open .csv file
func WriteCoordinateCSV(writer *csv.Writer, headersWritten *bool, record *models.Coordinate) {
	if !(*headersWritten) {
		headers := record.CSVHeaders()
		writer.Write(headers)
		*headersWritten = true
	}

	values := record.CSVValues()
	writer.Write(values)
}

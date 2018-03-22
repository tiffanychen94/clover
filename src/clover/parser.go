package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type DataParser interface {
	ParseSpec(string, string) error
	StoreData(string, string) error
	WriteLog(string)
	WriteErrorLog(string)
}

type parser struct {
	db       *sql.DB
	errorLog *os.File
	// Map from table name (spec) to map of field and width
	// Ideally I'd like to keep this out of memory and create either another table or store
	// externally to look up.
	widthSpecifications map[string][]int
}

func NewDataParser(db *sql.DB) (*parser, error) {
	errorLog, err := os.Create("parser_errors.log")
	if err != nil {
		return nil, err
	}
	newWidthMap := map[string][]int{}

	return &parser{
		db:                  db,
		errorLog:            errorLog,
		widthSpecifications: newWidthMap,
	}, nil
}

// WriteLog writes msg to the default source for log.
func (p *parser) WriteLog(msg string) {
	log.Printf(fmt.Sprintf("Parser: %s", msg))
}

// WriteErrorLog writes to an error log.
func (p *parser) WriteErrorLog(msg string) {
	_, err := p.errorLog.WriteString(fmt.Sprintf("%s\n", msg))
	if err != nil {
	}
}

//  Used to parse specs
func (p *parser) ParseSpec(location string, filename string) error {
	file, err := os.Open(location)
	if err != nil {
		fmt.Println("error opening file")
		return err
	}
	scanner := bufio.NewScanner(file)

	// skip the first column of the CSV
	scanner.Scan()
	fmt.Println(fmt.Sprintf("Table created with name %s", filename))
	_, err = p.db.Exec(fmt.Sprintf("CREATE TABLE %s()", filename))
	if err != nil {
		return err
	}
	p.widthSpecifications[filename] = []int{}
	for scanner.Scan() {
		currLine := scanner.Text()
		lineValues := strings.Split(currLine, ",")
		if len(lineValues) != 3 {
			return errors.New("Spec file not in expected format")
		}
		columnName := lineValues[0]
		columnWidth, _ := strconv.ParseInt(lineValues[1], 10, 64)
		columnType := lineValues[2]
		p.widthSpecifications[filename] = append(p.widthSpecifications[filename], int(columnWidth))
		fmt.Println(p.widthSpecifications)
		p.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", filename, columnName, columnType))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	file.Close()
	return nil
}

// Used to store data in appropriate format
func (p *parser) StoreData(location string, filename string) error {
	if p.widthSpecifications[filename] == nil || len(p.widthSpecifications[filename]) == 0 {
		return fmt.Errorf("No spec for this data type %s exists", filename)
	}
	file, err := os.Open(location)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		currLine := scanner.Text()
		lineValues := strings.Fields(currLine)
		fmt.Println(lineValues)
		insertValues := []string{}

		maxWidthInfo := p.widthSpecifications[filename]
		if len(lineValues) == 0 {
			fmt.Println("Invalid data file")
			return nil
		}
		i := 0
		currDataValue := lineValues[i]
		for i < len(maxWidthInfo) {
			if len(currDataValue) <= maxWidthInfo[i] {
				insertValues = append(insertValues, currDataValue)
				i++
				if i < len(maxWidthInfo) {
					currDataValue = lineValues[i]
				}
			} else {
				insertValues = append(insertValues, lineValues[i][:maxWidthInfo[i]])
				currDataValue = lineValues[i][maxWidthInfo[i]:]
				i++
			}
		}
		_, err := p.db.Exec(fmt.Sprintf("INSERT INTO %s VALUES%s", filename, enterValues(insertValues)))
		if err != nil {
			fmt.Println("Could not insert into db: %s", err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	file.Close()

	return nil
}

func enterValues(valueString []string) string {
	for i := 0; i < len(valueString); i++ {
		valueString[i] = fmt.Sprintf("'%s'", valueString[i])
	}
	return fmt.Sprintf("(%s)", strings.Join(valueString, ","))
}

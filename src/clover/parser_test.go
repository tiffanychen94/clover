package main

import (
	"errors"
	"fmt"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"testing"
)

const (
	testSpecName = "tester"
)

var (
	testSpecLocation = fmt.Sprintf("specs/%s.csv", testSpecName)
	testDataLocation = fmt.Sprintf("data/%s_2016-3-1.txt", testSpecName)
)

func TestParseSpecSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal("an error occured during mock db connection")
	}
	defer db.Close()

	sqlmock.NewRows([]string{"name", "valid", "count"}).
		AddRow("test", "1", "10")

	parser, err := NewDataParser(db)
	if err != nil {
		t.Fatal("could not initialize parser")
	}

	mock.ExpectExec(`CREATE TABLE tester()`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("ALTER TABLE tester ADD COLUMN name TEXT")
	mock.ExpectExec("ALTER TABLE tester ADD COLUMN valid BOOLEAN")
	mock.ExpectExec("ALTER TABLE tester ADD COLUMN count INTEGER")
	parser.ParseSpec(testSpecLocation, testSpecName)

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestParseSpecFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal("an error occured during mock db connection")
	}
	defer db.Close()

	sqlmock.NewRows([]string{"name", "valid", "count"}).
		AddRow("test", "1", "10")

	parser, err := NewDataParser(db)
	if err != nil {
		t.Fatal("could not initialize parser")
	}

	mock.ExpectExec(`CREATE TABLE tester()`).WillReturnError(errors.New("some error"))

	if parseErr := parser.ParseSpec(testSpecLocation, testSpecName); parseErr == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestStoreData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal("an error occured during mock db connection")
	}
	defer db.Close()

	sqlmock.NewRows([]string{"name", "valid", "count"})

	parser, err := NewDataParser(db)
	if err != nil {
		t.Fatal("could not initialize parser")
	}
	parser.widthSpecifications[testSpecName] = []int{10, 1, 3}

	mock.ExpectExec(`INSERT INTO tester VALUES(.+Foonyor.+,.+1.+,.+1.+)`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO tester VALUES(.+Barzane.+,.+0.+,.+-12.+)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO tester VALUES(.+Quuxitude.+,.+1.+,.+103.+)").WillReturnResult(sqlmock.NewResult(1, 1))

	parser.StoreData(testDataLocation, testSpecName)

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

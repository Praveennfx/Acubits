package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gorilla/mux"
)

var db *sql.DB

var server = "localhost"
var port = 1433
var user = "<your_username>"
var password = "<your_password>"
var database = "master"

func main() {
	initializeDB()
	handleRequests()
}

//Intializing DB connection
func initializeDB() {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Create connection pool
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	log.Println("Connected DB")
}

func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/search", getFromCoursera).Methods("POST")
	myRouter.HandleFunc("/search", getFromDBHandler).Methods("GET")
	log.Fatal(http.ListenAndServe(":8081", myRouter))
}

func getFromCoursera(w http.ResponseWriter, r *http.Request) {
	queryParams := r.URL.Query()
	searchText := strings.ToLower(queryParams["query"][0])
	sendReq := "https://api.coursera.org/api/courses.v1?q=search&query=" + searchText + "&fields=description"
	response, err := http.Get(sendReq)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err.Error())
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err.Error())
	}

	var responseObject Elements
	json.Unmarshal(responseData, &responseObject)

	data := responseObject.CourseraList
	for i := 0; i < len(data); i++ {
		temp := data[i]
		go putToCourseDB(temp)
	}
}

func getFromDBHandler(w http.ResponseWriter, r *http.Request) {
	queryParams := r.URL.Query()
	searchText := strings.ToLower(queryParams["query"][0])
	//get data form DB
	element, err := getFromDB(searchText)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err.Error())
	}
	w.WriteHeader(http.StatusOK)
	byteArray, err := json.Marshal(element)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatal(err)
	}
	w.Write(byteArray)
}

type Coursera struct {
	Search      string
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Author      []string `json:"instructorIds"`
}

type Elements struct {
	CourseraList []Coursera `json:"elements"`
}

func putToCourseDB(temp Coursera) {
	ctx := context.Background()
	var err error

	if db == nil {
		err = errors.New("CreateEmployee: db is null")
		log.Fatal(err.Error())
	}

	// Check if database is alive.
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}

	tsql := `
      INSERT INTO TestSchema.Course (Name, Description, Search) VALUES (@Name, @Description,@Search);
      select isNull(SCOPE_IDENTITY(), -1);
    `
	stmt, err := db.Prepare(tsql)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer stmt.Close()

	stmt.QueryRowContext(
		ctx,
		sql.Named("Name", temp.Name),
		sql.Named("Location", temp.Description),
		sql.Named("Search", temp.Search))

	for i := 0; i < len(temp.Author); i++ {
		author := temp.Author[i]
		// put author to another table
		putToAuthorDB(author, temp.Name)
	}

}

func putToAuthorDB(author string, name string) {
	ctx := context.Background()
	var err error

	if db == nil {
		err = errors.New("CreateEmployee: db is null")
		log.Fatal(err.Error())
	}

	// Check if database is alive.
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}

	tsql := `
      INSERT INTO TestSchema.Author (Author, Name) VALUES (@Author, @Name);
      select isNull(SCOPE_IDENTITY(), -1);
    `
	stmt, err := db.Prepare(tsql)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer stmt.Close()

	stmt.QueryRowContext(
		ctx,
		sql.Named("Name", name),
		sql.Named("Author", author))
}

func getFromDB(search string) (Elements, error) {
	var ElementList Elements

	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		return ElementList, err
	}

	tsql := "SELECT TOP 10 Search, Name, Description FROM TestSchema.Employees WHERE Search = @Search;"

	// Execute query
	rows, err := db.QueryContext(ctx, tsql, sql.Named("Search", search))
	if err != nil {
		return ElementList, err
	}

	defer rows.Close()

	var count int
	// Iterate through the result set.
	for rows.Next() {
		var course Coursera

		// Get values from row.
		err := rows.Scan(&course.Search, &course.Name, &course.Description)
		//Populate Author from Another table
		populateAuthor(&course)
		if err != nil {
			return ElementList, err
		}
		ElementList.CourseraList = append(ElementList.CourseraList, course)
		count++
	}

	return ElementList, nil
}

func populateAuthor(course *Coursera) {
	ctx := context.Background()

	// Check if database is alive.
	err := db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}

	tsql := "SELECT Author FROM TestSchema.Employees WHERE name = @name;"

	// Execute query
	rows, err := db.QueryContext(ctx, tsql, sql.Named("name", course.Name))
	if err != nil {
		log.Fatal(err.Error())
	}

	defer rows.Close()

	// Iterate through the result set.
	for rows.Next() {

		var author string
		// Get values from row.
		err := rows.Scan(&author)
		if err != nil {
			log.Fatal(err.Error())
		}
		course.Author = append(course.Author, author)
	}
}

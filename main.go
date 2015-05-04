package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jmcvetta/randutil"
	"github.com/seenickcode/gopherneo"
)

const maxUsers int = 100
const maxThingsPerUser int = 500
const relsPerUser int = 50

type User struct {
	Username string
}

type Thing struct {
	Title     string
	CreatedBy *User
}

func main() {

	resetFlag := flag.Bool("r", false, "reset data")
	flag.Parse()

	db, err := gopherneo.NewConnection("http://neo4j:1234@localhost:7474")
	if err != nil {
		panic(fmt.Errorf("couldn't connect to Neo4j %v", err))
	}
	db.SetRestCredentials("neo4j", "1234")
	db.DebugMode = false

	if *resetFlag == true {
		wipeAndReloadDatabase(db)
	}

	startTS := time.Now()
	readData(db)
	elapsed := time.Since(startTS)
	fmt.Println("elapsed: ", elapsed)
}

func wipeAndReloadDatabase(db *gopherneo.Connection) {
	fmt.Println("wiping and reload data")

	// clear database
	cy := "MATCH n OPTIONAL MATCH (n)-[r]-() DELETE n, r"
	props := &map[string]interface{}{}
	_, err := db.ExecuteCypher(cy, props)
	if err != nil {
		panic(err)
	}

	// create some indices
	cy = "CREATE INDEX ON :User(username)"
	props = &map[string]interface{}{}
	_, err = db.ExecuteCypher(cy, props)
	if err != nil {
		panic(err)
	}

	// load some random users and connecting them to a random
	// amount of things
	for j := 0; j < maxUsers; j++ {

		randUsername, _ := randutil.String(12, randutil.Alphabet)
		cy := "CREATE (n:User {props})"
		props := &map[string]interface{}{
			"props": &map[string]interface{}{"username": randUsername},
		}
		_, err = db.ExecuteCypher(cy, props)
		if err != nil {
			panic(err)
		}

		for k := 0; k < rand.Intn(maxThingsPerUser); k++ {
			randTitle, _ := randutil.String(12, randutil.Alphabet)
			cy = `
          MATCH (n:User) WHERE n.username={username} 
          CREATE (n)-[:HAS]->(t:Thing {props})
        `
			props = &map[string]interface{}{
				"username": randUsername,
				"props":    &map[string]interface{}{"title": randTitle},
			}
			_, err = db.ExecuteCypher(cy, props)
			if err != nil {
				panic(err)
			}
		}
	}

	// create rels
	for j := 0; j < maxUsers; j++ {
		skip1 := rand.Intn(maxUsers)
		for k := 0; k < relsPerUser; k++ {
			skip2 := rand.Intn(maxUsers)
			cy := `
          MATCH (u:User) 
          WITH u AS u1 SKIP {skip1} LIMIT 1
          MATCH (u:User) 
          WITH u1, u AS u2 SKIP {skip2} LIMIT 1 
          CREATE UNIQUE (u1)-[:KNOWS]->(u2)
        `
			props := &map[string]interface{}{
				"skip1": skip1,
				"skip2": skip2,
			}
			_, err = db.ExecuteCypher(cy, props)
			if err != nil {
				panic(err)
			}
		}
	}
}

func readData(db *gopherneo.Connection) {
	// fetch a news feed of 'things', sorted by date desc
	// of things created by me and what others have created
	fmt.Println("reading data")
	for j := 0; j < maxUsers; j++ {

		cy := `
      MATCH (u:User) WITH u SKIP {skip} LIMIT 1
      WITH u
      MATCH (u)-[:HAS]->(t:Thing) RETURN t, u
    `
		props := &map[string]interface{}{
			"skip": j,
		}
		cr, err := db.ExecuteCypher(cy, props)
		if err != nil {
			panic(err)
		}
		for _, row := range cr.Rows {
			t := Thing{}
			err := json.Unmarshal(*row[0], &t)
			if err != nil {
				panic(err)
			}
			u := User{}
			err = json.Unmarshal(*row[1], &u)
			if err != nil {
				panic(err)
			}
			t.CreatedBy = &u

			// report
			log.Printf("> Thing %v was created by %v\n", t.Title, t.CreatedBy.Username)
		}
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/firestore"
)

var (
	totalCnt      int
	errNoMoreDocs = fmt.Errorf("finished reading all the docs")
)

type userD struct {
	CustomerID string `firestore:"customer_id"`
	SomeData   string `firestore:"some_data"`
}

var batchSize = 10000

func main() {
	start := time.Now()
	var docCount int

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		log.Fatal("Expecting GOOGLE_CLOUD_PROJECT variable")
	}

	outFile, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("failed to create output.txt: %s\n", err)
	}
	logger := log.New(outFile, "", 0)

	//Create firestore client
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}

	usersColRef := client.Collection("test")
	i := 0
	lastCustID := ""

	for {
		log.Printf("running loop: %d\n", i)
		var docs []*firestore.DocumentSnapshot

		// Get the first batch, ordered by customerid
		if docCount == 0 {
			docs, err = usersColRef.OrderBy("customer_id", firestore.Asc).Where("customer_id", ">", "10").Limit(batchSize).Documents(ctx).GetAll()
		} else {
			docs, err = usersColRef.OrderBy("customer_id", firestore.Asc).Where("customer_id", ">", "").Limit(batchSize).StartAfter(lastCustID).Documents(ctx).GetAll()
		}
		if err != nil {
			if err == errNoMoreDocs {
				log.Println("done processing all the docs")
				break
			}
			log.Printf("failed to fetch documents: %s\n", err)
			break
		}
		if len(docs) == 0 {
			log.Println("no more docs to read")
			break
		}

		lastCustID = processDocs(docs, logger)

		log.Printf("Proccessed from %v to %v", docCount, docCount+batchSize)
		docCount += batchSize
		i++
	}

	log.Printf("Total users count: %d\n", totalCnt)
	log.Printf("Total time to finish %s\n", time.Since(start))
}

func processDocs(docs []*firestore.DocumentSnapshot, out *log.Logger) string {
	// loop through the docs and handle them
	var custid string
	for _, doc := range docs {
		docDup := doc
		totalCnt++

		// copy the data into userD struct
		d := userD{}
		if err := docDup.DataTo(&d); err != nil {
			log.Printf("Fail to decode user data: %s\n", err)
			continue
		}

		out.Printf("custid: %s\n", d.CustomerID)

		custid = d.CustomerID
	}

	return custid
}

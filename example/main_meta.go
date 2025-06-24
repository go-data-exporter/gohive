package main

import (
	"context"
	"log"

	"github.com/go-data-exporter/gohive"
	"github.com/go-data-exporter/gohive/hive_metastore"
)

func main() {
	configuration := gohive.NewMetastoreConnectConfiguration()
	connection, err := gohive.ConnectToMetastore("hm.example.com", 9083, "KERBEROS", configuration)
	if err != nil {
		log.Fatal(err)
	}
	database := hive_metastore.Database{
		Name:        "my_new_database",
		LocationUri: "/"}
	err = connection.Client.CreateDatabase(context.Background(), &database)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := connection.Client.GetAllDatabases(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	log.Println("databases ", databases)
	connection.Close()
}

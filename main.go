package main
import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"log"
)
func main() {

	var dbName, expression string

	flag.StringVar(&dbName, "d", "", "The name of the glue database")
	flag.StringVar(&expression, "e", "", "The pattern of the table names for deletion")
	flag.Parse()

	if dbName == "" || expression == "" {
		flag.Usage()
		return
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		// handle error
		log.Fatal(err)
	}

	fmt.Printf("Deleting tables from database '%s' matching expression '%s'\n", dbName, expression)

	glueSvc := glue.NewFromConfig(cfg)

	paginator := glue.NewGetTablesPaginator(glueSvc, &glue.GetTablesInput{
		Expression: aws.String(expression),
		DatabaseName: aws.String(dbName),
	})

	// loop through pages
	for paginator.HasMorePages() {
		tables, err := paginator.NextPage(context.TODO())

		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Deleting %d tables ...\n", len(tables.TableList))
		tableNames := make([]string, len(tables.TableList))
		for i, table := range tables.TableList {
			tableNames[i] = *table.Name
		}
		glueSvc.BatchDeleteTable(context.TODO(), &glue.BatchDeleteTableInput{
			DatabaseName: aws.String(dbName),
			TablesToDelete: tableNames,
		})
	}
}
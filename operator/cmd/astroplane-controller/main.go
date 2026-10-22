package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/JatinF/astroplane-airflow-control-plane/operator/internal/controller"
)

func main() {
	journal := flag.String("journal", "", "path to the controller journal")
	action := flag.String("action", "", "provision, upgrade, delete, dump, mark-deleting, or recover")
	file := flag.String("file", "", "AirflowCluster JSON for provision")
	name := flag.String("name", "", "cluster name")
	release := flag.String("release", "", "release name such as r2")
	flag.Parse()
	if *journal == "" || *action == "" {
		fmt.Fprintln(os.Stderr, "journal and action are required")
		os.Exit(2)
	}
	runtime, err := controller.Open(*journal)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	switch *action {
	case "provision":
		body, err := os.ReadFile(*file)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var desired controller.Cluster
		if err := json.Unmarshal(body, &desired); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		item, err := runtime.Provision(desired)
		exit(item, err)
	case "upgrade":
		item, err := runtime.Upgrade(*name, *release)
		exit(item, err)
	case "delete":
		item, err := runtime.Delete(*name)
		exit(item, err)
	case "mark-deleting":
		if err := runtime.MarkDeleting(*name); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "recover":
		names, err := runtime.Recover()
		exit(names, err)
	case "dump":
		item, ok := runtime.Get(*name)
		if !ok {
			fmt.Fprintln(os.Stderr, "cluster not found")
			os.Exit(2)
		}
		exit(item, nil)
	default:
		fmt.Fprintln(os.Stderr, "unknown action")
		os.Exit(2)
	}
}

func exit(value any, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}

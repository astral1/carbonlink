package main

import (
	"flag"
	"fmt"

	carbonlink "github.com/astral1/carbonlink/client"
)

func main() {
	metricName := flag.String("name", "", "metric full name")
	linkAddress := flag.String("host", "127.0.0.1:7002", "carbonlink tcp address")

	flag.Parse()

	link, err := carbonlink.NewCarbonlink(linkAddress, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer link.Close()

	reply := link.Probe(*metricName, 60)

	if len(reply.Datapoints) > 0 {
		fmt.Println(reply)
	}
}

package main

import (
	"flag"
	"fmt"

	"gopkg.in/astral1/carbonlink/client"
)

func main() {
	metricName := flag.String("name", "", "metric full name")
	linkAddress := flag.String("host", "127.0.0.1:7002", "carbonlink tcp address")

	flag.Parse()

	link, _ := carbonlink.NewCarbonlink(linkAddress)
	defer link.Close()

	link.SendRequest(metricName)
	reply := link.GetReply()

	if len(reply.Datapoints) > 0 {
		fmt.Println(reply.Datapoints[0])
	}
}

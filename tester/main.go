package main

import (
	"flag"
	"fmt"

	"github.com/astral1/carbonlink"
)

func main() {
	metricName := flag.String("name", "", "metric full name")
	linkAddress := flag.String("host", "127.0.0.1:7002", "carbonlink tcp address")

	flag.Parse()

	carbonlink, _ := NewCarbonlink(linkAddress)
	defer carbonlink.Close()

	carbonlink.SendRequest(metricName)
	reply := carbonlink.GetReply()

	if len(reply.Datapoints) > 0 {
		fmt.Println(reply.Datapoints[0])
	}
}

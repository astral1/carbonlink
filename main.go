package main

import (
	"flag"
	"fmt"
	"time"

	carbonlink "github.com/astral1/carbonlink/client"
)

func main() {
	metricName := flag.String("name", "", "metric full name")
	linkAddress := flag.String("host", "127.0.0.1:7002", "carbonlink tcp address")

	flag.Parse()

	pool := carbonlink.NewCarbonlinkPool(*linkAddress, 12)
	pool.SetTimeout(300 * time.Millisecond)
	go pool.Refresh()
	defer pool.Close()
	result := pool.Query(*metricName, 60)

	fmt.Print("By pool : ")
	fmt.Println(result)
}

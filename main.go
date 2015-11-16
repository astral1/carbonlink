package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	carbonlink "github.com/astral1/carbonlink/client"
)

func main() {
	metricName := flag.String("name", "", "metric full name")
	linkAddress := flag.String("host", "127.0.0.1:7002", "carbonlink tcp address")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	flag.Parse()

	var pool carbonlink.Carbonlink
	pool = carbonlink.NewCarbonlinkPool(*linkAddress, 12)
	pool.SetTimeout(300 * time.Millisecond)
	pool.(carbonlink.CarbonlinkPool).SetBaseRetryInterval(500 * time.Millisecond)
	pool.Start()
	defer pool.Close()

	for {
		bulkCount := r.Intn(10)
		for i := 0; i <= bulkCount+1; i++ {
			result := pool.Query(*metricName, 60)
			fmt.Print("By pool : ")
			fmt.Println(result)
		}

		time.Sleep(time.Duration(r.Intn(250)+250+1) * time.Millisecond)
	}
}

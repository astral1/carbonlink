package client

// Represent summerized cached results for a metric
type CarbonlinkPoints struct {
	// Result datapoints
	Datapoints map[int]float64
	// Result start from
	From int
	// Result finish until
	Until int
	// Interval for summarize
	Step int
}

// Create a empty points
func NewCarbonlinkPoints(step int) *CarbonlinkPoints {
	return &CarbonlinkPoints{Step: step, Datapoints: make(map[int]float64)}
}

// Convert from response of carbonlink to points
func (p *CarbonlinkPoints) ConvertFrom(reply *CarbonlinkReply) {
	for index, point := range reply.Datapoints {
		bucket := (int(point[0].(int64)) / p.Step) * p.Step
		value := point[1].(float64)

		p.Datapoints[bucket] = value
		if p.Until < bucket {
			p.Until = bucket
		}
		if index == 0 || p.From > bucket {
			p.From = bucket
		}
	}
}

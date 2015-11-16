package client

type CarbonlinkPoints struct {
	Datapoints map[int]float64
	From       int
	Until      int
	Step       int
}

func NewCarbonlinkPoints(step int) *CarbonlinkPoints {
	return &CarbonlinkPoints{Step: step, Datapoints: make(map[int]float64)}
}

func (p *CarbonlinkPoints) ConvertFrom(reply *CarbonlinkReply) {
	for index, point := range reply.Datapoints {
		bucket := (int(point[0].(int64)) / p.Step) * p.Step
		value := point[1].(float64)

		p.Datapoints[bucket] = value
		p.Until = bucket
		if index == 0 {
			p.From = bucket
		}
	}
}

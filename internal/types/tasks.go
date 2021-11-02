package types

type CreateQueueTask struct {
	GroupId int `json:"group_id"`
}

type SlowDownQueue struct {
	GroupId int `json:"group_id"`
	Ratio   int `json:"ratio"`
}

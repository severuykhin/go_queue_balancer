package group

import (
	"queue_balancer/internal/domain/group"
)

type GroupStorage struct {
}

func NewStorage() *GroupStorage {
	return &GroupStorage{}
}

func (gs *GroupStorage) GetMany(offsetId int, limit int) []group.Group {

	max := 10000
	res := []group.Group{}

	if offsetId >= max {
		return res
	}

	if offsetId > 1 {
		offsetId += 1
	}

	step := offsetId + limit

	for i := offsetId; i < step; i++ {
		g := group.Group{GroupId: i}
		res = append(res, g)
	}

	return res
}

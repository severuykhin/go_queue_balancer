package group

import (
	"queue_balancer/internal/domain/group"
)

type GroupStubStorage struct {
}

func NewStubStorage() *GroupStubStorage {
	return &GroupStubStorage{}
}

func (gs *GroupStubStorage) GetOne(groupId int) (*group.Group, error) {
	return &group.Group{
		GroupId: groupId,
		UserId:  1,
	}, nil
}

func (gs *GroupStubStorage) GetMany(offsetId int, limit int) ([]*group.Group, error) {

	max := 4000
	res := []*group.Group{}

	if offsetId >= max {
		return res, nil
	}

	if offsetId > 1 {
		offsetId += 1
	}

	step := offsetId + limit

	for i := offsetId; i < step; i++ {
		g := group.Group{GroupId: i}
		res = append(res, &g)
	}

	return res, nil
}

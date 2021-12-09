package group

import (
	"queue_balancer/internal/domain/group"
)

type GroupStorage struct {
}

func NewStorage() *GroupStorage {
	return &GroupStorage{}
}

func (gs *GroupStorage) GetByOffset(offsetId int, limit int) []group.Group {
	g1 := group.Group{GroupId: 1}
	g2 := group.Group{GroupId: 2}
	g3 := group.Group{GroupId: 3}
	g4 := group.Group{GroupId: 4}
	g5 := group.Group{GroupId: 5}

	return []group.Group{g1, g2, g3, g4, g5}
}

package group

import (
	"queue_balancer/internal/domain/group"
)

type Storage interface {
	GetMany(offsetId int, limit int) ([]*group.Group, error)
	GetOne(groupId int) (*group.Group, error)
}

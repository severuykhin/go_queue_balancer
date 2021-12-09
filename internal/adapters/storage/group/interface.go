package group

import (
	"queue_balancer/internal/domain/group"
)

type Storage interface {
	GetByOffset(offsetId int, limit int) []group.Group
}

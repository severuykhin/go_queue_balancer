package limits

import (
	"queue_balancer/internal/domain/limits"
)

type storage struct {
}

func NewStorage() *storage {
	return &storage{}
}

// Получить скорость потребления событий сообществом
// Если лимита в базе данных не обнаружено - без ограничений
func (s *storage) GetGroupRateLimit(groupId int) int {
	return limits.DEFAULT_GROUP_OPERATIONS_RATE_LIMIT
}

func (s *storage) GetAccountDailyLimit(userId int) int {
	return limits.DEFAULT_ACCOUNT_OPERATIONS_DAY_LIMIT
}

func (s *storage) GetCurrentAccountDailyLimit(userId int) int {
	return 1
}

func (s *storage) UpdateAccountDailyLimit(userId int) bool {
	return true
}

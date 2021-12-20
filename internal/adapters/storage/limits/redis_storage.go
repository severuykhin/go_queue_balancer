package limits

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

type StorageConfig struct {
	Addr     string
	Password string
	DB       int
}

type storage struct {
	client *redis.Client
}

func NewStorage(config StorageConfig) *storage {

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	return &storage{
		client,
	}
}

// Получить скорость потребления событий сообществом
func (s *storage) GetGroupRateLimit(groupId int) (int, error) {
	return 1, nil
}

// Получить дневной лимит операций аккаунта, которому принадлежит сообщество
func (s *storage) GetAccountOperationsDailyLimit(userId int) (int, error) {
	key := s.getAccountDailyLimitKey(userId)

	res, err := s.client.Get(context.TODO(), key).Result()

	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(res)

	if err != nil {
		return 0, err
	}

	return value, nil
}

// Уменьшить дневной лимит операци аккаунта, которому принадлежит сообщество
func (s *storage) DecreaseAccountOperationsDailyLimit(userId int, value int) (int, error) {
	key := s.getAccountDailyLimitKey(userId)

	res, err := s.client.DecrBy(context.TODO(), key, int64(value)).Result()

	if err != nil {
		return 0, err
	}

	return int(res), nil
}

func (s *storage) getAccountDailyLimitKey(userId int) string {
	return "user_limit_" + strconv.FormatInt(int64(userId), 10)
}

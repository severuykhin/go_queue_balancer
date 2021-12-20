package limits

type Storage interface {
	// Получить скорость потребления событий сообществом
	GetGroupRateLimit(groupId int) (int, error)
	// Получить дневной лимит операци аккаунта, которому принадлежит сообщество
	GetAccountOperationsDailyLimit(userId int) (int, error)
	// Уменьшить дневной лимит операци аккаунта, которому принадлежит сообщество
	DecreaseAccountOperationsDailyLimit(userId int, value int) (int, error)
}

package limits

type Storage interface {
	// Получить скорость потребления событий сообществом
	GetGroupRateLimit(groupId int) int
	// Получить дневной лимит операци аккаунта, которому принадлежит сообщество
	GetAccountDailyLimit(userId int) int
	// Получить текущее количество совершенных операций во всех сообществах внутри аккаунта
	GetCurrentAccountDailyLimit(userId int) int
	// Обновить дневной лимит операци аккаунта, которому принадлежит сообщество
	UpdateAccountDailyLimit(userId int) bool
}

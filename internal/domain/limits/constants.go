package limits

// Базовое количество операций в день для одного аккаунта
// Рассчитывается как - 150 сообщений x 4 (на бесплатном тарифе)
const DEFAULT_ACCOUNT_OPERATIONS_DAY_LIMIT = 600

// Базовая скорость обработки событий в сообществе
// -1 = без ограничений
const DEFAULT_GROUP_OPERATIONS_RATE_LIMIT = -1

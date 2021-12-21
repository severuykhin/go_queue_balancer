package publisher

type Publisher interface {
	Publish(groupId int, data []byte, headers map[string][]string) error
}

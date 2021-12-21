package publisher

import "fmt"

type StubPublisher struct {
	info map[int]int
}

func NewStubPublisher() *StubPublisher {
	publisher := StubPublisher{
		info: make(map[int]int),
	}
	return &publisher
}

func (np *StubPublisher) Publish(groupId int, data []byte, headers map[string][]string) error {
	fmt.Println("pusblish", groupId, string(data), headers)
	np.info[groupId]++
	return nil
}

func (np *StubPublisher) GetInfo() map[int]int {
	return np.info
}

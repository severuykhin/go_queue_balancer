package consumer

type Cosumer interface {
	ConsumeMainStream()
	Close()
}

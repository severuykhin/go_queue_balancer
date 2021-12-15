package consumer

import (
	"context"
)

type Cosumer interface {
	Run(ctx context.Context)
	Close()
}

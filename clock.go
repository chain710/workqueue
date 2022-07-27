package workqueue

import (
	"github.com/chain710/workqueue/internal/clock"
)

type Clock = clock.Clock

func NewClock() clock.Clock {
	return clock.RealClock{}
}

//goland:noinspection GoUnusedGlobalVariable
var NewFakeClock = clock.NewFakeClock

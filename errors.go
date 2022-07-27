package workqueue

import "errors"

var (
	InProcessingError  = errors.New("item in processing")
	IrreplaceableError = errors.New("irreplaceable item")
	ShutDownError      = errors.New("queue shut down")
)

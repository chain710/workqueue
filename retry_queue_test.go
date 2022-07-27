package workqueue

import (
	"fmt"
	"github.com/chain710/workqueue/internal/clock"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

type shouldRetryFunc func(object *indexObject) bool

type indexObject struct {
	index       string
	replaceable bool
	payload     int
	retried     bool
}

func (i *indexObject) IsReplaceable() bool {
	return i.replaceable
}

func (i *indexObject) Index() interface{} {
	return i.index
}

func (i *indexObject) Equal(o interface{}) bool {
	other, ok := o.(*indexObject)
	if !ok {
		return false
	}

	return i.payload == other.payload && i.index == other.index
}

func newIndexObject(index string, replaceable bool, payload int) *indexObject {
	return &indexObject{
		index:       index,
		replaceable: replaceable,
		payload:     payload,
	}
}

func durationPtr(duration time.Duration) *time.Duration {
	return &duration
}

func produceSequence(q RetryInterface, begin, end int) {
	for i := begin; i < end; i++ {
		_ = q.Add(&indexObject{index: strconv.Itoa(i), replaceable: true, payload: i})
	}
}

func consumeSequence(q RetryInterface, clk clock.Clock, maxDelay time.Duration, maxRetry time.Duration, result *sync.Map, shouldRetry shouldRetryFunc) {
	for {
		item, shutdown := q.Get()
		if shutdown {
			if item != nil {
				panic(fmt.Sprintf("shutdown queue get non-nil item %v", item))
			}
			return
		}

		obj := item.(*indexObject)
		result.Store(obj.payload, obj.payload)
		delay := time.Duration(rand.Int63n(int64(maxDelay)))
		if delay > 0 {
			clk.Sleep(delay)
		}

		var retry *time.Duration
		if !obj.retried && shouldRetry(obj) {
			retry = durationPtr(time.Duration(rand.Int63n(int64(maxRetry))))
			obj.retried = true
		}

		q.Done(item, retry)
	}
}

// retrySequence 随机选择randCount的item retry
func retrySequence(q RetryInterface, begin, end int, randCount int) {
	var keys []string
	for i := begin; i < end; i++ {
		keys = append(keys, strconv.Itoa(i))
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[j], keys[i] = keys[i], keys[j]
	})

	for _, key := range keys[:randCount] {
		q.Retry(key)
	}
}

func TestRetryableType_Basic(t *testing.T) {
	// 使用真实时钟, 测试生产，消费
	// 并发生产M*[0,N)元素
	// 消费随机delay[0~10) ms，mod10=0的retry一次
	// 最终消费完成，assert每个key都被消费

	maxDelay := 10 * time.Millisecond
	clk := &clock.RealClock{}
	q := NewRetryQueue("test", clk)
	producer := 10
	count := 200
	retryCount := 50
	var producerWG, consumerWG sync.WaitGroup
	producerWG.Add(producer)
	for i := 0; i < producer; i++ {
		go func() {
			produceSequence(q, 0, count)
			producerWG.Done()
		}()
	}

	consumer := 8
	var result sync.Map
	consumerWG.Add(consumer)
	for i := 0; i < consumer; i++ {
		go func() {
			consumeSequence(q, clk, maxDelay, maxDelay, &result,
				func(object *indexObject) bool {
					return object.payload%10 == 0
				})
			consumerWG.Done()
		}()
	}

	go func() {
		retrySequence(q, 0, count, retryCount)
	}()

	producerWG.Wait()
	// shutdown after all items added to queue
	q.ShutDown()
	consumerWG.Wait()
	actualCount := 0
	result.Range(func(key, value interface{}) bool {
		actualCount++
		return true
	})

	require.Equal(t, count, actualCount)
}

func TestRetryableType_Done(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	// add item and assert
	require.NoError(t, q.Add(newIndexObject("foo", true, 1)))
	require.Equal(t, 1, q.Len())
	item, shutdown := q.Get()
	require.False(t, shutdown)
	obj := item.(*indexObject)
	require.Equal(t, 1, obj.payload)

	// retry after 10s
	q.Done(item, durationPtr(10*time.Second))
	require.Equal(t, 1, q.Len())
	// shutdown, but we should still be able to get remaining items
	q.ShutDown()
	require.True(t, q.ShuttingDown())
	clk.Step(11 * time.Second)
	item, shutdown = q.Get()
	require.False(t, shutdown)
	require.Equal(t, 1, obj.payload)
	require.Equal(t, 0, q.Len())

	_, shutdown = q.Get()
	require.True(t, shutdown)
}

func TestRetryableType_Replaceable(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	// foo replaceable=true
	require.NoError(t, q.Add(newIndexObject("foo", true, 1)))
	require.NoError(t, q.Add(newIndexObject("foo", true, 2)))
	// bar replaceable=false
	require.NoError(t, q.Add(newIndexObject("bar", false, 3)))
	require.Equal(t, IrreplaceableError, q.Add(newIndexObject("bar", true, 4)))
	require.Equal(t, 2, q.Len())
	item, _ := q.Get() // should get foo-2
	q.Done(item, durationPtr(time.Second))
	obj := item.(*indexObject)
	require.Equal(t, 2, obj.payload)

	item, _ = q.Get() // should get bar-3
	q.Done(item, nil)
	obj = item.(*indexObject)
	require.Equal(t, 3, obj.payload)

	// foo-2 should not in queue
	require.Equal(t, 0, q.(*retryableType).qLen())
	// replace item in queue `foo`， immediately move item from retry to queue(should not clk.Step)
	require.NoError(t, q.Add(newIndexObject("foo", true, 5)))
	item, _ = q.Get()
	obj = item.(*indexObject)
	require.Equal(t, 5, obj.payload)
}

func TestRetryableType_Add(t *testing.T) {
	// add item, get but not done, repeat add should fail
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	require.NoError(t, q.Add(newIndexObject("foo", true, 1)))
	var item interface{}
	item, _ = q.Get()
	require.Equal(t, InProcessingError, q.Add(newIndexObject("foo", true, 2)))
	obj := item.(*indexObject)
	require.Equal(t, 1, obj.payload)
	q.Done(item, nil)
	q.ShutDown()
	_, shutdown := q.Get()
	require.True(t, shutdown)

	// add stopped queue, should fail
	require.Equal(t, ShutDownError, q.Add(newIndexObject("bar", true, 3)))
}

func TestRetryableType_AddEqual(t *testing.T) {
	// add equivalent item should success
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	require.NoError(t, q.Add(newIndexObject("foo", false, 1)))
	require.NoError(t, q.Add(newIndexObject("foo", false, 1)))
	require.Equal(t, 1, q.Len())
}

func TestRetryableType_Has(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	first := newIndexObject("foo", false, 1)
	require.NoError(t, q.Add(first))
	var item interface{}
	item, _ = q.Get()             // should get foo as in processing
	require.True(t, q.Has(first)) // Has in processing
	second := newIndexObject("bar", false, 2)
	require.NoError(t, q.Add(second))
	require.True(t, q.Has("bar")) // Has in queue, use index "bar"
	q.Done(item, durationPtr(time.Second))
	require.True(t, q.Has("foo")) // Has in retry, use index "bar"

	item, _ = q.Get() // should get bar
	q.Done(item, nil)
	require.False(t, q.Has("bar")) // negative, use index "bar"
	require.Equal(t, 0, q.(*retryableType).qLen())
}

func TestRetryableType_Delete(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	first := newIndexObject("foo", false, 1)
	require.NoError(t, q.Add(first))
	second := newIndexObject("bar", false, 2)
	require.NoError(t, q.Add(second))
	third := newIndexObject("baz", false, 3)
	require.NoError(t, q.Add(third))
	var item1, item2 interface{}
	item1, _ = q.Get() // get foo
	item2, _ = q.Get() // get bar
	// delete all, should error
	require.ErrorIs(t, q.Delete("bar", "foo", "baz"), InProcessingError)
	q.Done(item1, durationPtr(time.Second))
	q.Done(item2, nil)
	require.Equal(t, 2, q.Len()) // bar is done
	// delete all again,
	require.NoError(t, q.Delete("foo", "baz"))
	require.Equal(t, 0, q.Len())
}

func Test_retryableType_Retry(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	q := NewRetryQueue("test", clk)
	// add item and done(10sec)
	require.NoError(t, q.Add(newIndexObject("foo", true, 1)))
	// require.Equal(t, 1, q.Len())
	item, shutdown := q.Get()
	require.False(t, shutdown)
	q.Done(item, durationPtr(10*time.Second))
	require.Equal(t, 1, q.Len())

	// retry and get immediately
	q.Retry("foo")
	item, shutdown = q.Get()
	require.False(t, shutdown)
	obj := item.(*indexObject)
	require.Equal(t, 1, obj.payload)
	q.Done(item, nil)
	require.Equal(t, 0, q.Len())
}

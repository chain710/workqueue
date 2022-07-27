package workqueue

import (
	"github.com/chain710/workqueue/internal/clock"
	"github.com/chain710/workqueue/internal/wait"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSimpleWaitQueue(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)

	first := "foo"
	q.Add(first, 50*time.Millisecond)
	require.Equal(t, 1, q.Len())
	// first item not ready yet
	select {
	case <-q.C():
		require.Fail(t, "should not got notified")
	default:
		require.Equal(t, 0, len(q.Get()))
	}

	// step pass first add; retry ch event
	fakeClock.Step(60 * time.Millisecond)
	t.Logf("wait on chan")
	<-q.C()
	// require.NoError(t, waitChan(t, q.C(), false))
	items := q.Get()
	require.Equal(t, 1, len(items))
}

func TestWaitQueue_GetEmptyQueue(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)
	require.Equal(t, 0, len(q.Get()))
}

func TestWaitQueue_Remove(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)
	first := "foo"
	q.Add(first, 50*time.Millisecond)
	require.True(t, q.Remove(first))
	require.Equal(t, 0, q.Len())

	// remove not exist item
	second := "bar"
	require.False(t, q.Remove(second))

	// step after first add
	fakeClock.Step(60 * time.Millisecond)
	items := q.Get()
	require.Equal(t, 0, len(items))
}

func TestWaitQueue_Has(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)
	first := "foo"
	q.Add(first, 50*time.Millisecond)
	require.True(t, q.Has(first))

	fakeClock.Step(60 * time.Millisecond)
	_ = q.Get()
	require.False(t, q.Has(first))
}

func TestWaitQueue_Peek(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)
	require.Nil(t, q.Peek())

	first := "foo"
	q.Add(first, 50*time.Millisecond)
	require.Equal(t, first, q.Peek())
}

// add 若干item后stop，能get到剩余所有的元素
func TestWaitQueue_Stop(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)

	first := "foo"
	q.Add(first, 70*time.Millisecond)
	second := "bar"
	q.Add(second, 50*time.Millisecond)

	// step pass second add; retry ch event
	fakeClock.Step(60 * time.Millisecond)
	t.Logf("waiting first")
	<-q.C()
	items := q.Get()
	require.Equal(t, 1, len(items))
	require.Equal(t, "bar", items[0])

	// stop the queue, then we should still able to get item `foo`
	q.Stop()
	// step pass first add, should get foo
	fakeClock.Step(20 * time.Millisecond)
	t.Logf("waiting second")
	<-q.C()
	items = q.Get()
	require.Equal(t, 1, len(items))
	require.Equal(t, "foo", items[0])

	t.Logf("waiting stop")
	// 这里C()里可能还有元素
	require.NoError(t, waitChan(t, q.C(), true))
}

func TestWaitQueue_RepeatAdd(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	q := newWaitQueue(fakeClock)

	first := "foo"
	q.Add(first, 50*time.Millisecond)
	q.Add(first, 10*time.Millisecond) // overwrite last Add
	require.Equal(t, 1, q.Len())
	second := "bar"
	q.Add(second, 40*time.Millisecond)

	fakeClock.Step(20 * time.Millisecond)
	<-q.C()
	items := q.Get()
	require.Equal(t, 1, len(items))
	require.Equal(t, first, items[0])
}

func waitChan(t *testing.T, ch <-chan interface{}, expectClosed bool) error {
	return wait.Poll(10*time.Millisecond, 5*time.Second, func() (done bool, err error) {
		select {
		case item, ok := <-ch:
			t.Logf("chan len=%d. read item=%v. ok=%v", len(ch), item, ok)
			if expectClosed {
				return !ok, nil
			}
			return true, nil
		default:
			return false, nil
		}
	})
}

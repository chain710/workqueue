package workqueue

import (
	"container/heap"
	"github.com/chain710/workqueue/internal/clock"
	"sync"
	"time"
)

const maxWaitDuration = 10 * time.Second

/*
waitQueue 封装waitForPriorityQueue
避免item停留在chan中，难以删除
同时提供删除指定item的接口
与delayQueue不同在于：Get接口并不block，调用侧依赖C()返回的chan得到新元素通知
*/
type waitQueue struct {
	mu    sync.Mutex
	queue waitForPriorityQueue
	set   map[interface{}]*waitFor // for deletion
	ch    chan interface{}         // queue里有到期item时，写入ch，通知消费方, 当stopCh关闭，且set内元素为空时会关闭ch
	heap  chan struct{}            // 堆顶元素变化时通知(目前是Get,Add会写入heap chan)
	// heartbeat ensures we retry no more than maxWaitDuration before firing
	// 这里主要是防止多goroutine select ch，但是ch仅被一个goroutine观测到，我们的应用场景暂无问题，不过代码不多就先加上
	heartbeat clock.Ticker
	stopping  bool
	// clock tracks time for delayed firing
	clock Clock
}

func newWaitQueue(clock Clock) *waitQueue {
	q := waitQueue{
		queue:     waitForPriorityQueue{},
		set:       map[interface{}]*waitFor{},
		ch:        make(chan interface{}, 1),
		heap:      make(chan struct{}, 1),
		heartbeat: clock.NewTicker(maxWaitDuration),
		clock:     clock,
	}
	heap.Init(&q.queue)
	go q.loop()
	return &q
}

// C 返回ch, 得到通知时 Get 应有值返回,也有可能返回空，比如item在Get前被删除了，或者被别的Get取走了
func (w *waitQueue) C() <-chan interface{} {
	return w.ch
}

// Get return nil if no item in queue, return only ready items
func (w *waitQueue) Get() []interface{} {
	var entries []interface{}
	now := w.clock.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.queue.Len()
	if n == 0 {
		return entries
	}

	for w.queue.Len() > 0 {
		// peek
		entry := w.queue[0]
		if entry.readyAt.After(now) {
			break
		}

		entries = append(entries, entry.data)
		heap.Remove(&w.queue, entry.index)
		delete(w.set, entry.data)
	}

	if len(entries) > 0 {
		select {
		case w.heap <- struct{}{}:
		default:
		}
	}

	return entries
}

func (w *waitQueue) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.queue.Len()
}

// Peek returns the item at the beginning of the queue, without removing the
// item or otherwise mutating the queue. It is safe to call directly.
// Peek return nil if no item in queue
func (w *waitQueue) Peek() interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.queue.Len() == 0 {
		return nil
	}
	return w.queue[0].data
}

func (w *waitQueue) Add(x interface{}, delay time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return
	}
	readyAt := w.clock.Now().Add(delay)
	entry, exist := w.set[x]
	if exist {
		if entry.readyAt.After(readyAt) {
			entry.readyAt = readyAt
			heap.Fix(&w.queue, entry.index)
		}
		return
	}

	entry = &waitFor{
		data:    x,
		readyAt: readyAt,
	}
	heap.Push(&w.queue, entry)
	w.set[x] = entry
	// 保证w.heap里有值
	select {
	case w.heap <- struct{}{}:
	default:
	}
}

func (w *waitQueue) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopping = true
}

// Remove pop item x from heap, return exist or not
func (w *waitQueue) Remove(x interface{}) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry, exist := w.set[x]
	if !exist {
		return false
	}

	heap.Remove(&w.queue, entry.index)
	delete(w.set, x)
	return true
}

func (w *waitQueue) Has(x interface{}) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, exist := w.set[x]
	return exist
}

// nextReadyTimer 返回下次readyAt定时器
func (w *waitQueue) nextReadyTimer(now time.Time) clock.Timer {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.queue.Len() == 0 {
		return nil
	}
	entry := w.queue.Peek()
	if !entry.readyAt.After(now) {
		select {
		case w.ch <- entry.data:
		default:
		}
	}

	// 不要使用NewTimer，导致单元测试中nextReadyTimer时另一个携程clock step，导致now和NewTimer里的now不一致，最终卡主
	return w.clock.NewDeadlineTimer(entry.readyAt)
}

// loop 负责向 ch 写入，通知消费方
func (w *waitQueue) loop() {
	// Make a placeholder channel to use when there are no items in our list
	never := make(<-chan time.Time)
	for {
		if w.loopOnce(never) {
			return
		}
	}
}

// loopOnce return whether caller should break
func (w *waitQueue) loopOnce(never <-chan time.Time) bool {
	if w.shouldStop() {
		close(w.ch)
		w.heartbeat.Stop()
		return true
	}

	now := w.clock.Now()
	readyAtTimer := w.nextReadyTimer(now)
	defer func() {
		if readyAtTimer != nil {
			readyAtTimer.Stop()
		}
	}()
	readyAt := never
	if readyAtTimer != nil {
		readyAt = readyAtTimer.C()
	}
	select {
	case <-readyAt:
	case <-w.heartbeat.C():
	case <-w.heap:
		// 堆顶元素可能变化，重新计算
	}

	return false
}

func (w *waitQueue) shouldStop() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopping && w.queue.Len() == 0
}

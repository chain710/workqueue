package workqueue

import (
	"fmt"
	"github.com/chain710/workqueue/internal/clock"
	"sync"
	"time"
)

type Indexable interface {
	Index() interface{}
}

type Replaceable interface {
	IsReplaceable() bool
}

type Equivalent interface {
	Equal(interface{}) bool
}

type RetryInterface interface {
	// Add 对于无法加入队列的item返回false，可能原因：处理中，停止中，item不可覆盖；Add成功会清除 retry 队列里的对应item，即取消重试
	Add(item interface{}) error
	// Len 队列长度
	Len() int
	// Get 返回队列头部的item
	Get() (item interface{}, shutdown bool)
	// Done item结束，retry不为空时，表示item未执行完毕，需要一段时间后重试
	// 如果retry为空，那么会把delay队列里的item也删除，不影响 dirty set
	Done(item interface{}, retry *time.Duration)
	Has(item interface{}) bool // 是否已经在处理中或待处理
	ShutDown()
	ShuttingDown() bool
	Delete(items ...interface{}) error
	// Retry 立即重试：立即将item从retry移入queue，如item对应的key在retry中不存在，则什么也不做，入参item如果内容变化，不会体现在Get()
	Retry(items ...interface{})
}

func NewRetryQueue(name string, clk clock.Clock) RetryInterface {
	q := &retryableType{
		cond:                       sync.NewCond(&sync.Mutex{}),
		queue:                      []t{},
		dirty:                      map[t]t{},
		processing:                 set{},
		retry:                      newWaitQueue(clk),
		queueMetrics:               globalMetricsFactory.newQueueMetrics(name, clk),
		retryMetrics:               newRetryMetrics(name),
		unfinishedWorkUpdatePeriod: defaultUnfinishedWorkUpdatePeriod,
		clock:                      clk,
	}

	go q.updateUnfinishedWorkLoop()
	go q.retryLoop()
	return q
}

/*
retryableType 可重试的队列
包含如下集合
queue 即将处理的item/index
processing 当前正在处理的item/index，与 queue 无交集
retry 重试队列仅包含item/index，与 queue， processing 无交集
dirty 包含 retry 与 queue 的集合的item, 与 processing 无交集, Get 需要从 dirty 中获取item原值

简单来说, queue processing retry 三个集合无交集，这三个集合仅保存index
dirty=union(queue, retry), 集合中包含index与item
*/
type retryableType struct {
	cond *sync.Cond
	// queue defines the order in which we will work on items.
	queue []t

	// dirty defines all of the items that need to be processed. (including items in queue and retry)
	dirty valueSet

	// Things that are currently being processed are in the processing set.
	processing set

	shuttingDown               bool
	retry                      *waitQueue // wait里保存的是 dirty item的key，转入 queue 需要重新从 dirty get
	queueMetrics               queueMetrics
	retryMetrics               retryMetrics
	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.Clock
}

func (d *retryableType) Add(item interface{}) error {
	if item == nil {
		return nil
	}
	index := getIndex(item)
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	return d.add(index, item)
}

func (d *retryableType) Retry(items ...interface{}) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	moved := false
	for i := range items {
		index := getIndex(items[i])
		if d.moveItemToQueue(index) {
			moved = true
		}
	}
	if moved {
		d.cond.Signal()
	}
}

func (d *retryableType) retryAdd(index interface{}) {
	// 这里不看shuttingDown
	if d.processing.has(index) {
		panic(fmt.Sprintf("retry add item %v should not in processing set", index))
	}

	if _, ok := d.dirty.get(index); !ok {
		panic(fmt.Sprintf("retry add item %v should in dirty set", index))
	}

	d.queueMetrics.add(index)
	d.queue = append(d.queue, index)
	d.cond.Signal()
	return
}

func (d *retryableType) add(index, item interface{}) error {
	if d.shuttingDown {
		return ShutDownError
	}

	if d.processing.has(index) {
		return InProcessingError
	}

	if existItem, ok := d.dirty.get(index); ok {
		// if equal adjust from retry to queue, signal
		if isEqual(existItem, item) {
			if d.moveItemToQueue(index) {
				d.cond.Signal()
			}
			// dont add metric
			return nil
		}
		if !isReplaceable(existItem) {
			return IrreplaceableError
		}

		d.moveItemToQueue(index)
	} else {
		d.queue = append(d.queue, index)
	}

	d.queueMetrics.add(index)
	d.dirty.insert(index, item)
	d.cond.Signal()
	return nil
}

func (d *retryableType) Len() int {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	return len(d.queue) + d.retry.Len()
}

func (d *retryableType) Get() (item interface{}, shutdown bool) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	for d.shouldWait() {
		d.cond.Wait()
	}

	if len(d.queue) == 0 {
		// We must be shutting down.
		return nil, true
	}

	var index interface{}
	index, d.queue = d.queue[0], d.queue[1:]

	d.queueMetrics.get(index)
	d.processing.insert(index)
	var ok bool
	if item, ok = d.dirty.delete(index); !ok {
		panic(fmt.Sprintf("get index %v from queue not exist in dirty", index))
	}

	return item, false
}

func (d *retryableType) Done(item interface{}, retry *time.Duration) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	index := getIndex(item)
	d.queueMetrics.done(index)
	d.processing.delete(index)
	// panic if bug
	if d.retry.Has(index) {
		panic(fmt.Sprintf("item %v should not in retry", index))
	}
	if _, ok := d.dirty.get(index); ok {
		panic(fmt.Sprintf("item %v should not in dirty", index))
	}

	if retry != nil {
		d.addAfter(index, item, *retry)
	}
}

func (d *retryableType) Delete(items ...interface{}) error {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	// 必须所有item都未在执行，否则可能发生：item被Get，未处理完，但是被删除了，随后Add->Get，可能导致实际两个同key的item同时执行
	for _, item := range items {
		index := getIndex(item)
		if d.processing.has(index) {
			return InProcessingError
		}
	}

	// 均未在执行，可以安全删除
	for _, item := range items {
		index := getIndex(item)
		d.retry.Remove(index)
		d.dirty.delete(index)
		var keepQueue []t
		for _, qitem := range d.queue {
			if qitem == index {
				continue
			}

			keepQueue = append(keepQueue, qitem)
		}

		d.queue = keepQueue
	}

	return nil
}

func (d *retryableType) Has(item interface{}) bool {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	index := getIndex(item)
	_, dirtyHas := d.dirty.get(index)
	return d.processing.has(index) || dirtyHas
}

func (d *retryableType) addAfter(index, item interface{}, delay time.Duration) {
	if d.shuttingDown {
		return
	}
	if delay <= 0 {
		if err := d.add(index, item); err != nil {
			panic(fmt.Sprintf("retry add should not fail: %s", err))
		}
		return
	}
	d.retry.Add(index, delay)
	d.dirty.insert(index, item)
}

func (d *retryableType) ShutDown() {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	d.shuttingDown = true
	d.retry.Stop()
	d.cond.Broadcast()
}

func (d *retryableType) ShuttingDown() bool {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()

	return d.shuttingDown
}

func (d *retryableType) retryLoop() {
	// 注意：从retry里向queue转移，要受锁保护（与Add互斥），否则Add当中即将删除的，可能被重新放入queue
	for {
		for range d.retry.C() {
			d.cond.L.Lock()
			indexes := d.retry.Get()
			for _, index := range indexes {
				d.retryAdd(index)
			}

			d.cond.L.Unlock()
		}
	}
}

func (d *retryableType) updateUnfinishedWorkLoop() {
	t := d.clock.NewTicker(d.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			d.cond.L.Lock()
			defer d.cond.L.Unlock()
			if !d.shuttingDown {
				d.queueMetrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}

func (d *retryableType) shouldWait() bool {
	if d.shuttingDown {
		return d.retry.Len() > 0 && len(d.queue) == 0 // queue为空 但是 retry里还有
	} else {
		return len(d.queue) == 0
	}
}

func (d *retryableType) moveItemToQueue(index interface{}) bool {
	if !d.retry.Has(index) {
		return false
	}
	d.queue = append(d.queue, index)
	d.retry.Remove(index)
	return true
}

// qLen: for test return d.queue's length
func (d *retryableType) qLen() int {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	return len(d.queue)
}

func getIndex(item interface{}) interface{} {
	if i, ok := item.(Indexable); ok {
		return i.Index()
	}

	return item
}

func isReplaceable(item interface{}) bool {
	if r, ok := item.(Replaceable); ok {
		return r.IsReplaceable()
	}

	return false
}

func isEqual(a, b interface{}) bool {
	eqA, okA := a.(Equivalent)
	eqB, okB := b.(Equivalent)
	if okA {
		return eqA.Equal(b)
	} else if okB {
		return eqB.Equal(a)
	}

	// 未实现Equivalent接口的，认为不可比
	return false
}

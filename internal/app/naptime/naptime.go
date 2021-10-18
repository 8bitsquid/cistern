package naptime

import (
	"time"

	"github.com/Jeffail/tunny"
	"go.uber.org/zap"
)

type Condition interface {
	IsNapTime() (bool, error)
}

type Naptime struct {
	interval time.Duration
	workerPools []WorkerPool
	conditions []Condition
	stop chan struct{}
}

type WorkerPool struct {
	label string
	pool *tunny.Pool
	size int
}

func NewNaptime(interval time.Duration, conditions... Condition) *Naptime {
	nt := &Naptime{
		interval: interval,
		conditions: conditions,
		stop: make(chan struct{}),
	}

	return nt
}

func (nt *Naptime) Stop() {
	close(nt.stop)
}

func (nt *Naptime) AddWorkerPool(label string, pool *tunny.Pool, size int) {
	wp := WorkerPool{
		label: label,
		pool: pool,
		size: size,
	}
	nt.workerPools = append(nt.workerPools, wp)
	zap.S().Debugw("worker pool added to naptime", "pool", pool, "size", size)
}

func (nt *Naptime) MonitorConditions()  {
	go func() {
		for {
			select {
			case <-nt.stop:
				return
			case <-time.After(nt.interval):
				nt.checkConditions()
			}
		}
	}()
}

func(nt *Naptime) checkConditions() {
	for _, cond := range nt.conditions {

		sleepy, err := cond.IsNapTime()
		if err != nil {
			zap.S().Errorf("%+v", err)
		}
		
		for _, wp := range nt.workerPools {
			currSize := wp.pool.GetSize()
			if sleepy && currSize != 0 {
				zap.S().Warnf("Naptime for %s!", wp.label)
				wp.pool.SetSize(0)
			} else if currSize == 0 {
				zap.S().Warnf("Waking up %s!", wp.label)
				wp.pool.SetSize(wp.size)
			}
		}
	}
}


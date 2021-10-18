package naptime

import (
	"github.com/shirou/gopsutil/mem"
	"go.uber.org/zap"
)

type VirtMemNapConditions struct {
	memLimit uint64
}

func NewVirtMemNapConditions(limit uint64) *VirtMemNapConditions {
	return &VirtMemNapConditions{
		memLimit: limit,
	}
}

func (dnc *VirtMemNapConditions) IsNapTime() (bool, error) {

	mem, err := mem.VirtualMemory()
	if err != nil {
		zap.S().Errorf("error getting virtual memory usage: %v", err)
		return false, err
	}

	if mem.UsedPercent >= float64(dnc.memLimit) {
		zap.S().Warnf("virtual memory usage over %s", dnc.memLimit)
		return true, nil
	}
	return false, nil
}
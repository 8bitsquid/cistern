package naptime

import (
	"github.com/shirou/gopsutil/cpu"
	"go.uber.org/zap"
)

type CPUNapConditions struct {
	maxCPU float64
}

func NewCPUNapConditions(maxCPU float64) *CPUNapConditions {
	return &CPUNapConditions{
		maxCPU: maxCPU,
	}
}

func (cnp *CPUNapConditions) IsNapTime() (bool, error) {
	
	cpu, err := cpu.Percent(0, false)
	if err != nil {
		zap.S().Errorf("error getting cpu usage: %v", err)
		return false, err
	}

	if cpu[0] >= cnp.maxCPU {
		zap.S().Warnf("cpu usage over %s%", cnp.maxCPU)
		return true, nil
	}
	return false, nil
}
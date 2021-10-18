package naptime

import (
	"github.com/shirou/gopsutil/disk"
	"go.uber.org/zap"
)

type DiskNapConditions struct {
	sizeLimit uint64
	path string
}

func NewDiskNapConsitions(limit uint64, path string) *DiskNapConditions {
	return &DiskNapConditions{
		sizeLimit: limit,
		path: path,
	}
}

func (dnc *DiskNapConditions) IsNapTime() (bool, error) {

	dir, err := disk.Usage(dnc.path)
	if err != nil {
		zap.S().Errorf("error getting disk usage: %v", err)
		return false, err
	}

	if dir.Used >= dnc.sizeLimit {
		zap.S().Warnf("disk usage over %s", dnc.sizeLimit)
		return true, nil
	}
	return false, nil
}
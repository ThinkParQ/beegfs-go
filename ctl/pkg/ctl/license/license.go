package license

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	pl "github.com/thinkparq/protobuf/go/license"
	pm "github.com/thinkparq/protobuf/go/management"
	"go.uber.org/zap"
)

const (
	ContactEmail     = "licensing@thinkparq.com"
	PrefixCapacity   = "io.beegfs.capacity."
	PrefixNumServers = "io.beegfs.numservers."
	PrefixScope      = "io.beegfs.scope."
)

// Get license information the management
func GetLicense(ctx context.Context, reload bool) (*pl.GetCertDataResult, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	license, err := mgmtd.GetLicense(ctx, &pm.GetLicenseRequest{Reload: &reload})
	if err != nil {
		return nil, err
	}

	return license.CertData, nil
}

func TotalStorageCapacity(ctx context.Context) (uint64, error) {
	allTargets, err := target.GetTargets(ctx)
	if err != nil {
		return 0, err
	}

	var totalStorageCapacity uint64
	for _, t := range allTargets {
		if t.NodeType == beegfs.Storage {
			if t.TotalSpaceBytes == nil {
				return 0, fmt.Errorf("total space for node %s was unexpectedly nil", t.Node)
			}
			totalStorageCapacity += *t.TotalSpaceBytes
		}
	}
	return totalStorageCapacity, nil
}

// CheckIfOverStorageCapacityLimit accepts the io.beegfs.capacity. suffix and determines if it
// specifies a valid capacity limit in bytes then checks it against the total storage capacity.
// It avoids making unnecessary management RPCs if the capacity is not specified or unlimited.
func CheckIfOverStorageCapacityLimit(ctx context.Context, capacityLimit string) error {
	if capacityLimit == "unlimited" {
		return nil
	}

	cl, err := strconv.ParseUint(capacityLimit, 10, 64)
	if err != nil {
		log, _ := config.GetLogger()
		log.Debug("license specifies a capacity limit but it does not appear to be unlimited or a valid integer (ignoring)", zap.String("capacityLimit", capacityLimit))
		return nil
	}

	totalStorageCapacity, err := TotalStorageCapacity(ctx)
	if err != nil {
		log, _ := config.GetLogger()
		log.Debug("unable to fetch target information to verify available target capacity (ignoring)", zap.Error(err))
		return nil
	}

	if totalStorageCapacity > cl {
		// Note casting a uint64 to a float64 will lose precision with large numbers.
		return fmt.Errorf("system capacity (%s) exceeds licensed capacity (%s), contact %s for options to increase licensed capacity",
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(totalStorageCapacity), unitconv.IEC, 1)),
			fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(cl), unitconv.IEC, 1)),
			ContactEmail)
	}
	return nil
}

func GetTimeToExpiration(license *pl.GetCertDataResult) (remaining time.Duration, expireMsg string) {
	expireTimeWithoutGrace := license.Data.ValidUntil.AsTime().Add(-12 * time.Hour)
	remaining = time.Until(expireTimeWithoutGrace)
	return getTimeToExpirationString(remaining)
}

// getTimeToExpirationString is mostly needed for testing so the remaining time can be injected to
// avoid time-sensitive flaky tests if we worked with an actual license and calculated time.Until.
func getTimeToExpirationString(remaining time.Duration) (time.Duration, string) {
	expireMsg := ""
	switch {
	case remaining > 0 && remaining < time.Hour:
		expireMsg = "expires in less than an hour"
	case remaining > 0:
		expireMsg = fmt.Sprintf("expires in %s", roundToHoursOrDays(remaining))
	case remaining <= 0 && remaining > -time.Hour:
		expireMsg = "expired less than an hour ago"
	default:
		expireMsg = fmt.Sprintf("expired %s ago", roundToHoursOrDays(remaining))
	}
	return remaining, expireMsg
}

func roundToHoursOrDays(d time.Duration) (rounded string) {
	if d >= 24*time.Hour || d <= -24*time.Hour {
		count := int(d.Round(24*time.Hour) / (24 * time.Hour))
		if count < 0 {
			count = -count
		}
		unit := "days"
		if count == 1 {
			unit = "day"
		}
		return fmt.Sprintf("%d %s", count, unit)
	}
	count := int(d.Round(time.Hour) / time.Hour)
	if count < 0 {
		count = -count
	}
	unit := "hours"
	if count == 1 {
		unit = "hour"
	}
	return fmt.Sprintf("%d %s", count, unit)
}

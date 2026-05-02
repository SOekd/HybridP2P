package bandwidth

import (
	"fmt"
	"strconv"
	"strings"
)

func Parse(s string) (int64, error) {
	s = strings.ToLower(strings.TrimSpace(s))

	if s == "unlimited" || s == "0" || s == "inf" || s == "∞" {
		return 0, nil
	}

	var multiplier float64
	var numStr string

	switch {
	case strings.HasSuffix(s, "gbps") || strings.HasSuffix(s, "gbit"):
		multiplier = 1e9 / 8
		numStr = s[:len(s)-4]
	case strings.HasSuffix(s, "mbps") || strings.HasSuffix(s, "mbit"):
		multiplier = 1e6 / 8
		numStr = s[:len(s)-4]
	case strings.HasSuffix(s, "kbps") || strings.HasSuffix(s, "kbit"):
		multiplier = 1e3 / 8
		numStr = s[:len(s)-4]
	case strings.HasSuffix(s, "gb"):
		multiplier = 1e9 / 8
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "mb"):
		multiplier = 1e6 / 8
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "kb"):
		multiplier = 1e3 / 8
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "b"):
		multiplier = 1
		numStr = s[:len(s)-1]
	default:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot parse %q — try 10mb, 50mbps, or unlimited", s)
		}
		return v, nil
	}

	numStr = strings.TrimSpace(numStr)
	val, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q in bandwidth value", numStr)
	}
	if val < 0 {
		return 0, fmt.Errorf("bandwidth cannot be negative")
	}

	return int64(val * multiplier), nil
}

func Format(bps int64) string {
	switch {
	case bps == 0:
		return "unlimited"
	case bps >= 1e9/8:
		return fmt.Sprintf("%.0f Gbps", float64(bps)*8/1e9)
	case bps >= 1e6/8:
		return fmt.Sprintf("%.0f Mbps", float64(bps)*8/1e6)
	case bps >= 1e3/8:
		return fmt.Sprintf("%.0f Kbps", float64(bps)*8/1e3)
	default:
		return fmt.Sprintf("%d bps", bps*8)
	}
}

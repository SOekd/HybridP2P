//go:build !windows

package daemon

import (
	"os/exec"
	"syscall"
)

func configureDaemonProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}

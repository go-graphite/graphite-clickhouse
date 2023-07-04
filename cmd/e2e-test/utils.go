package main

import "os/exec"

func cmdExec(programm string, args ...string) (string, error) {
	cmd := exec.Command(programm, args...)
	out, err := cmd.CombinedOutput()

	return string(out), err
}

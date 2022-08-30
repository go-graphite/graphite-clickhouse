package main

import (
	"os/exec"
	"strings"
)

func containerExist(dockerBinary, name string) (bool, string) {
	if len(dockerBinary) == 0 {
		dockerBinary = "docker"
	}

	chInspect := []string{"inspect", "--format", "'{{.Name}}'", name}

	cmd := exec.Command(dockerBinary, chInspect...)
	out, err := cmd.CombinedOutput()
	s := strings.Trim(string(out), "\n")

	if err == nil {
		return true, s
	}

	return false, s
}

package main

import (
	"os/exec"
	"strings"
)

var DockerBinary string

func imageDelete(image, version string) (bool, string) {
	if len(DockerBinary) == 0 {
		panic("docker not set")
	}

	chArgs := []string{"rmi", image + ":" + version}

	cmd := exec.Command(DockerBinary, chArgs...)
	out, err := cmd.CombinedOutput()
	s := strings.Trim(string(out), "\n")

	if err == nil {
		return true, s
	}

	return false, err.Error() + ": " + s
}

func containerExist(name string) (bool, string) {
	if len(DockerBinary) == 0 {
		panic("docker not set")
	}

	chInspect := []string{"inspect", "--format", "'{{.Name}}'", name}

	cmd := exec.Command(DockerBinary, chInspect...)
	out, err := cmd.CombinedOutput()
	s := strings.Trim(string(out), "\n")

	if err == nil {
		return true, s
	}

	return false, err.Error() + ": " + s
}

func containerRemove(name string) (bool, string) {
	if len(DockerBinary) == 0 {
		panic("docker not set")
	}

	chInspect := []string{"rm", "-f", name}

	cmd := exec.Command(DockerBinary, chInspect...)
	out, err := cmd.CombinedOutput()
	s := strings.Trim(string(out), "\n")

	if err == nil {
		return true, s
	}

	return false, err.Error() + ": " + s
}

func containerExec(name string, args []string) (bool, string) {
	if len(DockerBinary) == 0 {
		panic("docker not set")
	}

	dCmd := []string{"exec", name}
	dCmd = append(dCmd, args...)

	cmd := exec.Command(DockerBinary, dCmd...)
	out, err := cmd.CombinedOutput()
	s := strings.Trim(string(out), "\n")

	if err == nil {
		return true, s
	}

	return false, err.Error() + ": " + s
}

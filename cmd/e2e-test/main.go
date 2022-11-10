package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"

	"go.uber.org/zap"
)

type MainConfig struct {
	Test *TestSchema `toml:"test"`
}

func IsDir(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func expandDir(dirname string, paths *[]string) error {
	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			if err = expandDir(path.Join(dirname, file.Name()), paths); err != nil {
				return err
			}
		} else {
			ext := path.Ext(file.Name())
			if ext == ".toml" {
				*paths = append(*paths, path.Join(dirname, file.Name()))
			}
		}
	}

	return nil
}

func expandFilename(filename string, paths *[]string) error {
	if len(filename) == 0 {
		return nil
	}
	isDir, err := IsDir(filename)
	if err == nil {
		if isDir {
			if err = expandDir(filename, paths); err != nil {
				return err
			}
		} else {
			*paths = append(*paths, filename)
		}
	}
	return err
}

func main() {
	_, filename, _, _ := runtime.Caller(0)
	rootDir := path.Dir(path.Dir(path.Dir(filename))) // carbon-clickhouse repositiry root dir

	config := flag.String("config", "", "toml configuration file or dir where toml files is searched (recursieve)")
	verbose := flag.Bool("verbose", false, "verbose")
	breakOnError := flag.Bool("break", false, "break and wait user response if request failed")
	cleanup := flag.Bool("cleanup", false, "delete containers if exists before start")
	flag.Parse()
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	DockerBinary = os.Getenv("DOCKER_E2E")
	if DockerBinary == "" {
		DockerBinary = "docker"
	}
	if *cleanup {
		if exist, _ := containerExist(CchContainerName); exist {
			if ok, out := containerRemove(CchContainerName); !ok {
				logger.Fatal("failed to cleanup",
					zap.String("container", CchContainerName),
					zap.String("error", out),
				)
			}
		}
		if exist, _ := containerExist(ClickhouseContainerName); exist {
			if ok, out := containerRemove(ClickhouseContainerName); !ok {
				logger.Fatal("failed to cleanup",
					zap.String("container", ClickhouseContainerName),
					zap.String("error", out),
				)
			}
		}
		if *config == "" {
			return
		}
	}

	var configs []string
	err = expandFilename(*config, &configs)
	if err != nil {
		logger.Fatal(
			"config",
			zap.Error(err),
		)
	}
	if len(configs) == 0 {
		logger.Fatal("config should be non-null")
	}

	failed := 0
	total := 0
	verifyCount := 0
	verifyFailed := 0
	for _, config := range configs {
		testFailed, testTotal, vCount, vFailed := runTest(config, rootDir, *verbose, *breakOnError, logger)
		failed += testFailed
		total += testTotal
		verifyCount += vCount
		verifyFailed += vFailed
	}

	if failed > 0 {
		logger.Error("tests ended",
			zap.String("status", "failed"),
			zap.Int("test_count", total),
			zap.Int("test_failed", failed),
			zap.Int("checks", verifyCount),
			zap.Int("failed", verifyFailed),
			zap.Int("configs", len(configs)),
		)
		os.Exit(1)
	} else {
		logger.Info("tests ended",
			zap.String("status", "success"),
			zap.Int("test_count", total),
			zap.Int("test_failed", failed),
			zap.Int("checks", verifyCount),
			zap.Int("failed", verifyFailed),
			zap.Int("configs", len(configs)),
		)
	}
}

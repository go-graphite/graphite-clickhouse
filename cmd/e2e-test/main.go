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

func expand(filename string, paths *[]string) error {
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
	flag.Parse()
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	var configs []string
	err = expand(*config, &configs)
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
	for _, config := range configs {
		testFailed, testTotal := runTest(config, rootDir, *verbose, logger)
		failed += testFailed
		total += testTotal
	}

	if failed > 0 {
		logger.Error("tests ended",
			zap.String("status", "failed"),
			zap.Int("count", total),
			zap.Int("failed", failed),
			zap.Int("configs", len(configs)),
		)
		os.Exit(1)
	} else {
		logger.Info("tests ended",
			zap.String("status", "success"),
			zap.Int("count", total),
			zap.Int("configs", len(configs)),
		)
	}
}

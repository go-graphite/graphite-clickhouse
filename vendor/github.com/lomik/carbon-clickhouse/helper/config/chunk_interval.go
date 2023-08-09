package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type chunkIntervalRule struct {
	unhandled int
	interval  time.Duration
}

type ChunkAutoInterval struct {
	rules           []chunkIntervalRule
	defaultInterval time.Duration
}

func NewChunkAutoInterval() *ChunkAutoInterval {
	return &ChunkAutoInterval{
		rules: make([]chunkIntervalRule, 0),
	}
}

func (c *ChunkAutoInterval) SetDefault(v time.Duration) {
	c.defaultInterval = v
}

// UnmarshalText from TOML
func (c *ChunkAutoInterval) UnmarshalText(p []byte) error {
	s := strings.TrimSpace(string(p))
	c.rules = make([]chunkIntervalRule, 0)
	if s == "" {
		return nil
	}
	a := strings.Split(s, ",")
	for i := 0; i < len(a); i++ {
		kv := strings.Split(strings.TrimSpace(a[i]), ":")
		if len(kv) != 2 {
			return fmt.Errorf("can't parse %#v", s)
		}
		k, err := strconv.Atoi(kv[0])
		if err != nil {
			return fmt.Errorf("can't parse %#v: %s", s, err.Error())
		}
		v, err := time.ParseDuration(kv[1])
		if err != nil {
			return fmt.Errorf("can't parse %#v: %s", s, err.Error())
		}
		c.rules = append(c.rules, chunkIntervalRule{k, v})
	}

	sort.SliceStable(c.rules, func(i, j int) bool { return c.rules[i].unhandled < c.rules[j].unhandled })

	return nil
}

func (c *ChunkAutoInterval) MarshalText() ([]byte, error) {
	s := make([]string, 0)
	for i := 0; i < len(c.rules); i++ {
		s = append(s, fmt.Sprintf("%d:%s", c.rules[i].unhandled, c.rules[i].interval.String()))
	}
	return []byte(strings.Join(s, ",")), nil
}

func (c *ChunkAutoInterval) GetInterval(unhandledCount int) time.Duration {
	var i int
	for i = 0; i < len(c.rules); i++ {
		if unhandledCount < c.rules[i].unhandled {
			break
		}
	}

	if i > 0 {
		return c.rules[i-1].interval
	}

	return c.defaultInterval
}

func (c *ChunkAutoInterval) GetDefault() time.Duration {
	return c.defaultInterval
}

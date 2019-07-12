package finder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlainFromTaggedFinderAbs(t *testing.T) {
	assert := assert.New(t)

	eq := func(name, value string) TaggedTerm {
		return TaggedTerm{Op: TaggedTermEq, Key: name, Value: value}
	}

	join := func(terms ...TaggedTerm) []TaggedTerm {
		return terms
	}

	f := makePlainFromTagged(join(
		eq("__name__", "graphite"),
		eq("rename", "cpu_usage"),
		eq("target", "telegraf.*.cpu.usage"),
		eq("node1", "host"),
	))

	assert.NotNil(f)

	table := [][2]string{
		{
			"telegraf.localhost.cpu.usage",
			`cpu_usage?host=localhost&metric=telegraf.localhost.cpu.usage`,
		},
	}

	for _, c := range table {
		assert.Equal(c[1], string(f.Abs([]byte(c[0]))))
	}

}

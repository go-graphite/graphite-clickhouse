package config

import (
	"fmt"
	"io/fs"
	"regexp"
	"regexp/syntax"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessDataTables(t *testing.T) {
	type in struct {
		table       DataTable
		tableLegacy string
	}
	type out struct {
		tables []DataTable
		err    error
	}
	type ctx map[string]bool

	regexpCompileWrapper := func(re string) *regexp.Regexp {
		r, _ := regexp.Compile(re)
		return r
	}

	tests := []struct {
		name string
		in   in
		out  out
	}{
		{
			name: "legacy table only",
			in: in{
				tableLegacy: "graphite.data",
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "legacy and normal tables",
			in: in{
				table:       DataTable{Table: "graphite.new_data"},
				tableLegacy: "graphite.data",
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.new_data",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
					{
						Table:      "graphite.data",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "fail to compile TargetMatchAll",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAll: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAll: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to compile TargetMatchAny",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAny: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAny: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to compile TargetMatchAny",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAny: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAny: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to read xml rollup",
			in: in{
				table: DataTable{Table: "graphite.data", RollupConf: "/some/file/that/does/not/hopefully/exists/on/the/disk"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", RollupConf: "/some/file/that/does/not/hopefully/exists/on/the/disk"}},
				&fs.PathError{Op: "open", Path: "/some/file/that/does/not/hopefully/exists/on/the/disk", Err: syscall.ENOENT},
			},
		},
		{
			name: "unknown context",
			in: in{
				table: DataTable{Table: "graphite.data", Context: []string{"unexpected"}},
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						Context:    []string{"unexpected"},
						ContextMap: ctx{},
					},
				},
				fmt.Errorf("unknown context \"unexpected\""),
			},
		},
		{
			name: "check all works",
			in: in{
				table: DataTable{
					Table:                  "graphite.data",
					Reverse:                true,
					TargetMatchAll:         "^.*[asdf][.].*",
					TargetMatchAny:         "^.*{a|s|d|f}[.].*",
					RollupConf:             "none",
					RollupDefaultFunction:  "any",
					RollupDefaultPrecision: 61,
					RollupUseReverted:      true,
					Context:                []string{"prometheus"},
				},
				tableLegacy: "table",
			},
			out: out{
				[]DataTable{
					{
						Table:                  "graphite.data",
						Reverse:                true,
						TargetMatchAll:         "^.*[asdf][.].*",
						TargetMatchAny:         "^.*{a|s|d|f}[.].*",
						TargetMatchAllRegexp:   regexpCompileWrapper("^.*[asdf][.].*"),
						TargetMatchAnyRegexp:   regexpCompileWrapper("^.*{a|s|d|f}[.].*"),
						RollupConf:             "none",
						RollupDefaultFunction:  "any",
						RollupDefaultPrecision: 61,
						RollupUseReverted:      true,
						Context:                []string{"prometheus"},
						ContextMap:             ctx{"prometheus": true},
					},
					{
						Table:      "table",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "unknown context",
			in: in{
				table: DataTable{Table: "graphite.data", Context: []string{"unexpected"}},
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						Context:    []string{"unexpected"},
						ContextMap: ctx{},
					},
				},
				fmt.Errorf("unknown context \"unexpected\""),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := New()
			if test.in.table.Table != "" {
				cfg.DataTable = []DataTable{test.in.table}
			}
			if test.in.tableLegacy != "" {
				cfg.ClickHouse.DataTableLegacy = test.in.tableLegacy
			}
			err := cfg.ProcessDataTables()
			if err != nil {
				assert.Equal(t, test.out.err, err)
				return
			}
			assert.Equal(t, len(test.out.tables), len(cfg.DataTable))
			// it's difficult to check rollup.Rollup because Rules.updated field
			// We explicitly don't check it here
			for i := range cfg.DataTable {
				test.out.tables[i].Rollup = nil
				cfg.DataTable[i].Rollup = nil
			}
			assert.Equal(t, test.out.tables, cfg.DataTable)
		})
	}
}

func TestKnownDataTableContext(t *testing.T) {
	assert.Equal(t, map[string]bool{ContextGraphite: true, ContextPrometheus: true}, knownDataTableContext)
}

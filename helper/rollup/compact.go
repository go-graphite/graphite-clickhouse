package rollup

import (
	"fmt"
	"strconv"
	"strings"
)

/*
compact form of rollup rules for tests

regexp;function;age:precision,age:precision,...
*/

func parseCompact(body string) (*Rules, error) {
	lines := strings.Split(body, "\n")
	patterns := make([]Pattern, 0)

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		p2 := strings.LastIndexByte(line, ';')
		if p2 < 0 {
			return nil, fmt.Errorf("can't parse line: %#v", line)
		}
		p1 := strings.LastIndexByte(line[:p2], ';')
		if p1 < 0 {
			return nil, fmt.Errorf("can't parse line: %#v", line)
		}
		regexp := strings.TrimSpace(line[:p1])
		function := strings.TrimSpace(line[p1+1 : p2])
		retention := make([]Retention, 0)

		if strings.TrimSpace(line[p2+1:]) != "" {
			arr := strings.Split(line[p2+1:], ",")

			for _, r := range arr {
				p := strings.Split(r, ":")
				if len(p) != 2 {
					return nil, fmt.Errorf("can't parse line: %#v", line)
				}

				age, err := strconv.Atoi(strings.TrimSpace(p[0]))
				if err != nil {
					return nil, err
				}

				precision, err := strconv.Atoi(strings.TrimSpace(p[1]))
				if err != nil {
					return nil, err
				}

				retention = append(retention, Retention{Age: uint32(age), Precision: uint32(precision)})
			}
		}

		patterns = append(patterns, Pattern{Regexp: regexp, Function: function, Retention: retention})
	}

	return &Rules{Pattern: patterns}, nil
}

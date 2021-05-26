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

func parseCompact(body string, auto bool) (*Rules, error) {
	lines := strings.Split(body, "\n")
	patterns := make([]Pattern, 0)

	for _, line := range lines {
		var ruleType RuleType
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
		if len(regexp) > 8 && regexp[0] == '<' && regexp[1] == '!' && regexp[7] == '>' {
			typeStr := regexp[1:7]
			switch typeStr {
			case "!ALL_T":
				ruleType = RuleAll
			case "!PLAIN":
				ruleType = RulePlain
			case "!TAG_R":
				ruleType = RuleTaggedRegex
			//case "!TAG_T":
			//	ruleType = RuleTagged
			default:
				return nil, fmt.Errorf("not realised rule type for line: %#v", line)
			}
			regexp = regexp[8:]
		} else {
			if ruleType == RuleAuto {
				if auto && len(regexp) > 0 {
					ruleType = AutoDetectRuleType(regexp)
				} else {
					ruleType = RuleAll
				}
			}
		}
		function := strings.TrimSpace(line[p1+1 : p2])
		retention := make([]Retention, 0)

		if strings.TrimSpace(line[p2+1:]) != "" {
			arr := strings.Split(line[p2+1:], ",")

			for _, r := range arr {
				p := strings.Split(r, ":")
				if len(p) != 2 {
					return nil, fmt.Errorf("can't parse line: %#v", line)
				}

				age, err := strconv.ParseUint(strings.TrimSpace(p[0]), 10, 32)
				if err != nil {
					return nil, err
				}

				precision, err := strconv.ParseUint(strings.TrimSpace(p[1]), 10, 32)
				if err != nil {
					return nil, err
				}

				retention = append(retention, Retention{Age: uint32(age), Precision: uint32(precision)})
			}
		}

		patterns = append(patterns, Pattern{RuleType: ruleType, Regexp: regexp, Function: function, Retention: retention})
	}

	return (&Rules{Pattern: patterns}).compile()
}

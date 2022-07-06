package rollup

import (
	"encoding/xml"
)

/*
<graphite_rollup>
 	<pattern>
 		<regexp>click_cost</regexp>
 		<function>any</function>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>60</precision>
 		</retention>
 	</pattern>
 	<default>
 		<function>max</function>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>300</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
 	</default>
</graphite_rollup>
*/

type ClickhouseRollupXML struct {
	Rules RulesXML `xml:"graphite_rollup"`
}

type RetentionXML struct {
	Age       uint32 `xml:"age"`
	Precision uint32 `xml:"precision"`
}

type PatternXML struct {
	RuleType  RuleType        `xml:"type"`
	Regexp    string          `xml:"regexp"`
	Function  string          `xml:"function"`
	Retention []*RetentionXML `xml:"retention"`
}

type RulesXML struct {
	Pattern []*PatternXML `xml:"pattern"`
	Default *PatternXML   `xml:"default"`
}

func (r *RetentionXML) retention() Retention {
	return Retention{Age: r.Age, Precision: r.Precision}
}

func (p *PatternXML) pattern() Pattern {
	result := Pattern{
		RuleType:  p.RuleType,
		Regexp:    p.Regexp,
		Function:  p.Function,
		Retention: make([]Retention, 0, len(p.Retention)),
	}

	for _, r := range p.Retention {
		result.Retention = append(result.Retention, r.retention())
	}

	return result
}

func parseXML(body []byte, auto bool) (*Rules, error) {
	r := &RulesXML{}
	err := xml.Unmarshal(body, r)
	if err != nil {
		return nil, err
	}

	// Maybe we've got Clickhouse's graphite.xml?
	if r.Default == nil && r.Pattern == nil {
		y := &ClickhouseRollupXML{}
		err = xml.Unmarshal(body, y)
		if err != nil {
			return nil, err
		}
		r = &y.Rules
	}

	patterns := make([]Pattern, 0, uint64(len(r.Pattern))+4)
	for _, p := range r.Pattern {
		patterns = append(patterns, p.pattern())
		for i := range patterns {
			if patterns[i].RuleType == RuleAuto {
				if auto {
					patterns[i].RuleType = AutoDetectRuleType(patterns[i].Regexp)
				} else {
					patterns[i].RuleType = RuleAll
				}
			}
		}
	}

	if r.Default != nil {
		r.Default.RuleType = RuleAll
		patterns = append(patterns, r.Default.pattern())
	}

	return (&Rules{Pattern: patterns}).compile()
}

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
		Regexp:    p.Regexp,
		Function:  p.Function,
		Retention: make([]Retention, 0, len(p.Retention)),
	}

	for _, r := range p.Retention {
		result.Retention = append(result.Retention, r.retention())
	}

	return result
}

func parseXML(body []byte) (*Rules, error) {
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

	patterns := make([]Pattern, 0, len(r.Pattern)+4)
	for _, p := range r.Pattern {
		patterns = append(patterns, p.pattern())
	}

	if r.Default != nil {
		patterns = append(patterns, r.Default.pattern())
	}

	// if defaultFunction != "" {
	// 	patterns = append(patterns, Pattern{
	// 		Regexp:   "",
	// 		Function: defaultFunction,
	// 	})
	// }

	// if defaultPrecision != 0 {
	// 	patterns = append(patterns, Pattern{
	// 		Regexp: "",
	// 		Retention: []Retention{
	// 			Retention{Age: 0, Precision: defaultPrecision},
	// 		},
	// 	})
	// }

	// patterns = append(patterns, Pattern{
	// 	Regexp:    "",
	// 	Function:  superDefaultFunction,
	// 	Retention: superDefaultRetention,
	// })

	result := &Rules{
		Pattern: patterns,
		// Updated: time.Now().Unix(),
	}

	// err = result.compile()
	// if err != nil {
	// 	return nil, err
	// }

	return result, nil
}

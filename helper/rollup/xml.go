package rollup

import (
	"encoding/xml"
	"io/ioutil"
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
	Rules Rules `xml:"graphite_rollup"`
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

func ParseXML(body []byte, defaultPrecision uint32, defaultFunction string) (*Rules, error) {
	r := &RulesXML{}
	err := xml.Unmarshal(body, r)
	if err != nil {
		return nil, err
	}

	// Maybe we've got Clickhouse's graphite.xml?
	if r.Default == nil && r.Pattern == nil {
		y := &ClickhouseRollup{}
		err = xml.Unmarshal(body, y)
		if err != nil {
			return nil, err
		}
		r = &y.Rules
	}

	err = r.compile()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func ReadFromXMLFile(filename string, defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	rollupConfBody, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	rules, err := ParseXML(rollupConfBody, defaultPrecision, defaultFunction)
	if err != nil {
		return nil, err
	}

	return &Rollup{
		rules:            rules,
		defaultPrecision: defaultPrecision,
		defaultFunction:  defaultFunction,
	}, nil
}

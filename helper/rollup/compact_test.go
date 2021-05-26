package rollup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCompact(t *testing.T) {
	config := `
	click_cost;any;0:3600,86400:60
	<!PLAIN>\.max$;max;0:3600,86400:60
	<!TAG_R>\.max\?;max;0:3600
	\.min$;min;0:3600,86400:60
	\.min\?;min;0:3600
	env=cloud;avg;0:3600
	;avg;0:60,3600:300,86400:3600`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedPlain, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedTagged, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleAll, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	assert := assert.New(t)
	r, err := parseCompact(config, false)
	assert.NoError(err)
	assert.Equal(expected, r)

	assert.Equal(len(expected.patternPlain), 6)
	assert.Equal(expectedPlain.Pattern, r.patternPlain)

	assert.Equal(len(expected.patternTagged), 6)
	assert.Equal(expectedTagged.Pattern, r.patternTagged)
}

func TestParseCompactAutoDetect(t *testing.T) {
	config := `
	click_cost;any;0:3600,86400:60
	<!PLAIN>\.max$;max;0:3600,86400:60
	<!TAG_R>\.max\?;max;0:3600
	\.min$;min;0:3600,86400:60
	\.min\?;min;0:3600
	env=cloud;avg;0:3600
	;avg;0:60,3600:300,86400:3600`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RulePlain, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedPlain, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RulePlain, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedTagged, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	assert := assert.New(t)
	r, err := parseCompact(config, true)
	assert.NoError(err)
	assert.Equal(expected, r)

	assert.Equal(expectedPlain.Pattern, r.patternPlain)

	assert.Equal(expectedTagged.Pattern, r.patternTagged)
}

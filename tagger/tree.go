package tagger

type Tree struct {
	Next  [256]*Tree
	Rules []*Rule
}

func (t *Tree) Add(prefix []byte, rule *Rule) {
	x := t

	for i := 0; i < len(prefix); i++ {
		if x.Next[prefix[i]] == nil {
			x.Next[prefix[i]] = &Tree{}
		}

		x = x.Next[prefix[i]]
	}

	if x.Rules == nil {
		x.Rules = make([]*Rule, 0)
	}

	x.Rules = append(x.Rules, rule)
}

func (t *Tree) AddSuffix(suffix []byte, rule *Rule) {
	x := t

	for i := len(suffix) - 1; i >= 0; i-- {
		if x.Next[suffix[i]] == nil {
			x.Next[suffix[i]] = &Tree{}
		}

		x = x.Next[suffix[i]]
	}

	if x.Rules == nil {
		x.Rules = make([]*Rule, 0)
	}

	x.Rules = append(x.Rules, rule)
}

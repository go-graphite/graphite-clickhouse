package tagger

type Tree struct {
	Next  [256]*Tree
	Rules []*Tag
}

func (t *Tree) Add(prefix []byte, rule *Tag) {
	x := t

	for i := 0; i < len(prefix); i++ {
		if x.Next[prefix[i]] == nil {
			x.Next[prefix[i]] = &Tree{}
		}

		x = x.Next[prefix[i]]
	}

	if x.Rules == nil {
		x.Rules = make([]*Tag, 0)
	}

	x.Rules = append(x.Rules, rule)
}

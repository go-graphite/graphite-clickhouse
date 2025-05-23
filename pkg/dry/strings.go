package dry

// RemoveEmptyStrings removes empty strings from list and returns truncated slice
func RemoveEmptyStrings(stringList []string) []string {
	rm := 0

	for i := 0; i < len(stringList); i++ {
		if stringList[i] == "" {
			rm++
			continue
		}

		if rm > 0 {
			stringList[i-rm] = stringList[i]
		}
	}

	return stringList[:len(stringList)-rm]
}

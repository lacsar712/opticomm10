package trail


func FanoutMark(targets []string, fn func(string) error) (done bool, err error) {
	if len(targets) == 0 {
		return true, nil
	}
	var first error
	for _, t := range targets {
		if e := fn(t); e != nil && first == nil {
			first = e
		}
	}
	return true, first
}

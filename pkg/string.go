package pkg

func stringPointer(args ...string) *string {
	var out string
	
	for _, arg := range args {
		out = out + arg
	}
	
	return &out
}

package seekstream

func subSlice(p []byte, size int) []byte {
	if size < len(p) {
		return p[:size]
	}

	return p
}

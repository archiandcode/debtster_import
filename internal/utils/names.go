package utils

import "strings"

func ParseFullName(fullname string) (last, first, middle string) {
	fullname = strings.Join(strings.Fields(strings.TrimSpace(fullname)), " ")
	parts := strings.Split(fullname, " ")

	if len(parts) > 0 {
		last = parts[0]
	}
	if len(parts) > 1 {
		first = parts[1]
	}
	if len(parts) > 2 {
		middle = parts[2]
	}

	return
}

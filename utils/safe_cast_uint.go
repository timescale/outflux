package utils

import "fmt"

// SafeCastUInt attempts to cast an uint to a int or returns an error in the case of an overflow
func SafeCastUInt(num uint) (int, error) {
	numInt := int(num)
	if numInt < 0 || uint(numInt) != num {
		return -1, fmt.Errorf("%v could not be safely expressed as a signed int, it's too large", num)
	}

	return numInt, nil
}

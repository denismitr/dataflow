package superstream

import (
	"fmt"
	"strings"
)

var (
	ErrSkip = fmt.Errorf("must skip item")
)

type MapReduceError []error

func (mpErr MapReduceError) Error() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d map reduce errors: ", len(mpErr)))
	for i, err := range mpErr {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(err.Error())
	}
	return b.String()
}

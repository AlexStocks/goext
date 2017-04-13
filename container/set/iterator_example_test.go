// refs: https://github.com/deckarep/golang-set 20170413, branch master, latest version v1.6
package gxset

import (
	"fmt"
)

import (
	"github.com/davecgh/go-spew/spew"
)

type YourType struct {
	Name string
}

func ExampleIterator() {
	set := NewSetFromSlice([]interface{}{
		&YourType{Name: "Alise"},
		&YourType{Name: "Bob"},
		&YourType{Name: "John"},
		&YourType{Name: "Nick"},
	})

	fmt.Println("set:", spew.Sdump(set))

	var found *YourType = nil
	it := set.Iterator()

	for elem := range it.C {
		if elem.(*YourType).Name == "John" {
			found = elem.(*YourType)
			it.Stop()
		}
	}

	fmt.Printf("Found %+v\n", found)

	// Output: Found &{Name:John}
}

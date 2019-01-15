package gxstrings

import (
	"testing"
)

// go test -v -timeout 30s github.com/AlexStocks/goext/strings -run TestMerge
func TestMerge(t *testing.T) {
	s1 := []string{"hello0", "hello1"}
	s2 := []string{}

	s3 := Merge(s1, s2)
	if len(s3) != 2 {
		t.Errorf("the length of s3 should be 2")
	}
	t.Logf("s3 %#v", s3)

	s1 = []string{"hello0", "hello1"}
	s2 = []string{"world"}

	s3 = Merge(s1, s2)
	if len(s3) != 3 {
		t.Errorf("the length of s3 should be 3")
	}
	t.Logf("s3 %#v", s3)
}

func TestContains(t *testing.T) {
	arr := []string{"hello0", "hello1"}
	array := Strings2Ifs(arr)
	flag := Contains(array, "hello0")
	if !flag {
		t.Errorf("element should be in string array")
	}
}

func TestIsSameStringArray(t *testing.T) {
	arr1 := []string{"hello0", "hello1"}
	arr2 := []string{"hello0", "hello1"}
	arr3 := []string{"hello2", "hello1"}

	if !IsSameStringArray(arr1, arr2) {
		t.Errorf("two arrays should be the same")
	}

	if IsSameStringArray(arr3, arr2) {
		t.Errorf("two arrays should not be the same")
	}
}

func TestIsSubset(t *testing.T) {
	arr1 := []string{"hello0", "hello1"}
	array1 := Strings2Ifs(arr1)
	arr2 := []string{"hello0", "hello1"}
	array2 := Strings2Ifs(arr2)
	if !IsSubset(array1, array2) {
		t.Errorf("the subset return value should be true")
	}

	arr3 := []string{"hello2", "hello1"}
	array3 := Strings2Ifs(arr3)
	if IsSubset(array3, array2) {
		t.Errorf("the subset return value should not be true")
	}
}

func TestSub(t *testing.T) {
	arr1 := []string{"hello0", "hello1"}
	arr2 := []string{"hello0", "hello1"}
	if len(Sub(arr1, arr2)) != 0 {
		t.Errorf("the sub return value length should be 0")
	}

	arr3 := []string{"hello2", "hello1"}
	if len(Sub(arr3, arr2)) != 1 {
		t.Errorf("the sub return value length should be 1")
	}
}

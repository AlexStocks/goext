package gxxor

import "testing"

func TestXor(t *testing.T) {
	xor := NewXor([]byte("hello"))
	text := "1234567890-abcdefg-hijklmn-opqrst-uvwxyz"
	encText := xor.Encrypt(text)
	if decTxt, err := xor.Decrypt(encText); decTxt != text || err != nil {
		t.Fatalf("text:%q, decrypt text:%s, err:%s", text, decTxt, err)
	}
}

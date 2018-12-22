package gxxor

import "testing"

func TestXor(t *testing.T) {
	xor := NewXor([]byte("hello"))
	text := "1234567890-abcdefg-hijklmn-opqrst-uvwxyz"
	encText := xor.Encrypt(text)
	if decTxt := xor.Decrypt(encText); decTxt != text {
		t.Fatalf("text:%q, decrypt text:%s", text, decTxt)
	}
}

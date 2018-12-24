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

func TestXorBase64(t *testing.T) {
	// txt := "helloworld,你好世界"
	txt := "123456789"
	x := NewXor([]byte("hello123"))
	encryptTxt := x.EncryptInBase64(txt)

	decryptTxt, err := x.DecryptInBase64(encryptTxt)
	if err != nil {
		t.Errorf("xor.DecryptInBase64(txt:%s) = error:%s", encryptTxt, err)
	}
	t.Logf("decrypted txt:%s", decryptTxt)
}

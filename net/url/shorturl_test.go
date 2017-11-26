package gxurl

import (
	"testing"
)

func TestGenGitioShortURL(t *testing.T) {
	shortURL, err := GenGitioShortURL("https://github.com/alexstocks/goext")
	if err != nil {
		t.Errorf("error:%#v", err)
	}

	t.Logf("short url:%s", shortURL)
}

func TestGenSinaShortURL(t *testing.T) {
	shortURL, err := GenSinaShortURL("https://github.com/alexstocks/goext")
	if err != nil {
		t.Errorf("error:%#v", err)
	}

	t.Logf("short url:%s", shortURL)
}

func TestGenSinaShortURLByGoogd(t *testing.T) {
	shortURL, err := GenSinaShortURLByGoogd("https://github.com/alexstocks/goext")
	if err != nil {
		t.Errorf("error:%#v", err)
	}

	t.Logf("short url:%s", shortURL)
}

func TestGenBaiduShortURL(t *testing.T) {
	shortURL, err := GenBaiduShortURL("https://github.com/alexstocks/goext")
	if err != nil {
		t.Errorf("error:%#v", err)
	}

	t.Logf("short url:%s", shortURL)
}

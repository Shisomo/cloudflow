package test

import (
	"testing"
)

func Test_Cli(t *testing.T) {
	t.Log("start test ...")
	tests := []struct {
		name string
	}{
		{name: "test_cli"},
	}
	for _, tt := range tests {
		t.Log(tt.name)
	}
}

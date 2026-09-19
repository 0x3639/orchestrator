package app

import "testing"

func TestHasLegacyPassphrase(t *testing.T) {
	cases := map[string]bool{
		`{"DataPath":"/x"}`:                               false,
		`{"ProducerKeyFilePassphrase":""}`:                false,
		`{"ProducerKeyFilePassphrase":"secret"}`:          true,
		`{"ProducerKeyFilePassphrase":123}`:               true,
		`not json`:                                        false,
		`{"ProducerKeyFilePassphraseFile":"/run/secret"}`: false,
	}
	for in, want := range cases {
		if got := hasLegacyPassphrase([]byte(in)); got != want {
			t.Errorf("hasLegacyPassphrase(%s) = %v, want %v", in, got, want)
		}
	}
}

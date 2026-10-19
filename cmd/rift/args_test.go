package main

import "testing"

func TestPersistenceIsOptIn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		persist bool
	}{
		{name: "default"},
		{name: "opt in", args: []string{"--persist"}, persist: true},
		{name: "explicit one shot", args: []string{"--no-persist"}},
		{name: "disable wins", args: []string{"--persist", "--no-persist"}},
		{name: "enable wins", args: []string{"--no-persist", "--persist"}, persist: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseArgs(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.persist != tc.persist {
				t.Fatalf("persist = %v, want %v", cfg.persist, tc.persist)
			}
		})
	}
}

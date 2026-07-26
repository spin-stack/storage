package dst

// Drain and placement scenarios (§28.1) live here rather than in scenarios.go so an
// increment working on the drain never contends with one working on recovery or on
// the harness itself for the same list. Empty is a legitimate state: the entries
// arrive with the increment that proves them.

func drainScenarios() []MandatoryScenario { return nil }

func drainCheckers() []Checker { return nil }

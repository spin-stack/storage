package dst

// Harness-level scenarios: fault injection driven by the seed, resource exhaustion,
// and anything whose subject is the simulation itself rather than one subsystem.
// See scenarios_drain.go for why the list is split by area.

func harnessScenarios() []MandatoryScenario { return nil }

func harnessCheckers() []Checker { return nil }

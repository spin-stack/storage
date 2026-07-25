package violating

// A separate file so it can import "time" under an alias (import decls are
// per-file). The analyzer must resolve by the real package path, not the local
// import name, so this must still be flagged.
import clock "time"

func aliasedImport() {
	_ = clock.Now() // want `direct use of time\.Now`
}

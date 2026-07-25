// Package violating exercises the analyzer: every forbidden primitive here must
// produce a diagnostic because this package is not under internal/simio.
package violating

import (
	"net"
	"os"
	"time"
)

func useTime() {
	_ = time.Now()                  // want `direct use of time\.Now`
	_ = time.Since(time.Now())      // want `direct use of time\.Since` `direct use of time\.Now`
	time.Sleep(time.Second)         // want `direct use of time\.Sleep`
	_ = time.After(time.Second)     // want `direct use of time\.After`
	_ = time.Tick(time.Second)      // want `direct use of time\.Tick`
	_ = time.NewTimer(time.Second)  // want `direct use of time\.NewTimer`
	_ = time.NewTicker(time.Second) // want `direct use of time\.NewTicker`
}

func useNet() {
	_, _ = net.Dial("tcp", "x:1")         // want `direct use of net\.Dial`
	_, _ = net.DialTimeout("tcp", "x", 0) // want `direct use of net\.DialTimeout`
	_, _ = net.Listen("tcp", ":0")        // want `direct use of net\.Listen`
}

func useOS() {
	_, _ = os.Open("f")           // want `direct use of os\.Open`
	_, _ = os.Create("f")         // want `direct use of os\.Create`
	_, _ = os.OpenFile("f", 0, 0) // want `direct use of os\.OpenFile`
	_, _ = os.ReadFile("f")       // want `direct use of os\.ReadFile`
	_ = os.WriteFile("f", nil, 0) // want `direct use of os\.WriteFile`
}

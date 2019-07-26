package raft

import "log"

// Debugging
const Debug = 1

func init() {
  log.SetFlags(log.Lmicroseconds | log.Ldate)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

package wal

import (
	"gobase/src/shared"
)

type StateType int

const (
	StateBegin StateType = iota
	StateCommit
	StateAbort
	StateUpdate
)

var stateType = map[StateType]string{
	StateBegin:  "BEGIN",
	StateCommit: "COMMIT",
	StateAbort:  "ABORT",
	StateUpdate: "UPDATE",
}

type LogRecord interface {
	GetState() StateType
}

type Record struct {
	LSN           int
	TransactionID int
	State         StateType
}

type BeginRecord struct {
	Record
}

type CommitRecord struct {
	Record
}

type AbortRecord struct {
	Record
}

type UpdateRecord struct {
	Record

	RID      shared.RID
	OldValue []byte
	NewValue []byte
}

type WAL struct {
	FinalLSN int
	Logs     []LogRecord
}

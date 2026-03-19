package wal

func (record *Record) GetState() StateType {
	return record.State
}

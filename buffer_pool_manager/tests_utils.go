package buffer_pool_manager

import (
	"os"
	"testing"

	"gobase/disk_manager"

	"github.com/stretchr/testify/require"
)

func newTestDiskManager(t *testing.T) (*disk_manager.DiskManager, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "bpm_test")
	require.NoError(t, err)

	dm, err := disk_manager.NewDiskManager(tmpFile.Name())
	require.NoError(t, err)

	cleanup := func() {
		dm.Close()
		os.Remove(tmpFile.Name())
	}

	return dm, cleanup
}

func newTestBufferPoolManager(t *testing.T, poolSize int) (*BufferPoolManager, func()) {
	t.Helper()

	dm, cleanup := newTestDiskManager(t)
	bpm := NewBufferPoolManager(dm, poolSize)

	return bpm, cleanup
}

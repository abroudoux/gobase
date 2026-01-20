package disk_manager

import (
	"gobase/shared"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDiskManager(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "disk_manager_test")
	require.NoError(t, err)
	filePath := tmpFile.Name()
	defer os.Remove(filePath)
	tmpFile.Close()

	dm, err := NewDiskManager(filePath)
	require.NoError(t, err)
	assert.NotNil(t, dm)
	assert.Equal(t, uint32(0), dm.NumPages, "empty file should have 0 pages")
	assert.Equal(t, shared.PAGE_SIZE, dm.PageSize)

	require.NoError(t, dm.File.Close())

	err = os.WriteFile(filePath, make([]byte, shared.PAGE_SIZE), 0644)
	require.NoError(t, err)

	dm2, err := NewDiskManager(filePath)
	require.NoError(t, err)
	assert.NotNil(t, dm2)
	assert.Equal(t, uint32(1), dm2.NumPages, "file with 1 page should have NumPages=1")
	assert.Equal(t, shared.PAGE_SIZE, dm2.PageSize)

	info, err := dm2.File.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(shared.PAGE_SIZE), info.Size())

	require.NoError(t, dm2.File.Close())
}
package disk_manager

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDiskManager(t *testing.T, numPages uint32) (*DiskManager, func()) {
	t.Helper()

	file, err := os.CreateTemp("", "disk_manager_test")
	require.NoError(t, err)

	cleanup := func() {
		file.Close()
		os.Remove(file.Name())
	}

	return &DiskManager{
		File:     file,
		PageSize: 8,
		NumPages: numPages,
	}, cleanup
}

func TestReadPage(t *testing.T) {
	initialData := []byte("ABCDEFGH")
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	require.NoError(t, dm.WritePage(0, initialData))

	data, err := dm.ReadPage(0)
	require.NoError(t, err)
	assert.Equal(t, int(dm.PageSize), len(data))
	assert.Equal(t, initialData, data)
}

func TestReadPage_PageIDDoesNotExist(t *testing.T) {
	data := []byte("ABCDEFGH")
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	_, err := dm.File.WriteAt(data, 0)
	require.NoError(t, err)

	_, err = dm.ReadPage(1)
	require.ErrorIs(t, err, ErrPageDoesNotExist)
}

func TestWritePage(t *testing.T) {
	initialData := []byte("ABCDEFGH")
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	require.NoError(t, dm.WritePage(0, initialData))

	data, err := os.ReadFile(dm.File.Name())
	require.NoError(t, err)
	assert.Equal(t, initialData, data)
}

func TestWritePage_PageIDDoesNotExist(t *testing.T) {
	data := []byte("ABCDEFGH")
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	err := dm.WritePage(1, data)
	require.ErrorIs(t, err, ErrPageDoesNotExist)
}

func TestWritePage_DataSizeDoesNotMatchPageSize(t *testing.T) {
	data := []byte("ABCDEFGHI")
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	err := dm.WritePage(0, data)
	require.ErrorIs(t, err, ErrInvalidPageDataSize)
}

func TestAllocatePage(t *testing.T) {
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	newPageID, err := dm.AllocatePage()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), newPageID)
	assert.Equal(t, uint32(2), dm.NumPages)

	data, err := dm.ReadPage(newPageID)
	require.NoError(t, err)

	expectedData := make([]byte, dm.PageSize)
	assert.Equal(t, expectedData, data)
}

func TestAllocatePage_FileWriteError(t *testing.T) {
	dm, cleanup := newTestDiskManager(t, 1)
	defer cleanup()

	dm.File.Close()

	_, err := dm.AllocatePage()
	require.ErrorIs(t, err, ErrAllocatePageFailed)
	assert.Equal(t, uint32(1), dm.NumPages)
}

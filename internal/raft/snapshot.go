package raft

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"

	hraft "github.com/hashicorp/raft"
	"github.com/shrec21/engram/internal/store"
)

// fsmSnapshot implements raft.FSMSnapshot by tarring a Pebble checkpoint.
type fsmSnapshot struct {
	checkpointDir string
}

// Persist writes the checkpoint directory as a tar archive into the sink.
func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	tw := tar.NewWriter(sink)
	err := filepath.Walk(s.checkpointDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(s.checkpointDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("tar checkpoint: %w", err)
	}

	if err := tw.Close(); err != nil {
		sink.Cancel()
		return fmt.Errorf("close tar: %w", err)
	}
	return sink.Close()
}

// Release cleans up the temporary checkpoint directory.
func (s *fsmSnapshot) Release() {
	os.RemoveAll(s.checkpointDir)
}

// restoreSnapshot extracts a tar archive into destDir and replaces the store.
func restoreSnapshot(st *store.Store, rc io.ReadCloser) error {
	tmpDir, err := os.MkdirTemp("", "engram-snap-restore-*")
	if err != nil {
		return fmt.Errorf("mktemp: %w", err)
	}

	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			os.RemoveAll(tmpDir)
			return fmt.Errorf("tar read: %w", err)
		}

		target := filepath.Join(tmpDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				os.RemoveAll(tmpDir)
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				os.RemoveAll(tmpDir)
				return err
			}
			f, err := os.Create(target)
			if err != nil {
				os.RemoveAll(tmpDir)
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				os.RemoveAll(tmpDir)
				return err
			}
			f.Close()
		}
	}

	return st.ReplaceFromDir(tmpDir)
}

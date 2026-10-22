package controller

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Journal is the controller's durable desired state.
// A missing journal with a sibling .tmp file is restored. That covers a
// crash between deleting the previous file and renaming the temp file on
// Windows, where rename does not replace an existing file.

type Journal struct {
	path     string
	Clusters map[string]Cluster `json:"clusters"`
}

func Load(path string) (*Journal, error) {
	if err := restoreTemp(path); err != nil {
		return nil, err
	}
	journal := &Journal{path: path, Clusters: map[string]Cluster{}}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return journal, nil
	}
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return journal, nil
	}
	if err := json.Unmarshal(body, journal); err != nil {
		return nil, err
	}
	journal.path = path
	if journal.Clusters == nil {
		journal.Clusters = map[string]Cluster{}
	}
	return journal, nil
}

func restoreTemp(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp := path + ".tmp"
	if _, err := os.Stat(tmp); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (j *Journal) Save() error {
	if err := os.MkdirAll(filepath.Dir(j.path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, j.path); err != nil {
		_ = os.Remove(j.path)
		return os.Rename(tmp, j.path)
	}
	return nil
}

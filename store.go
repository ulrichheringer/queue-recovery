package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

type IDStore struct {
	path string
	mu   sync.Mutex
	ids  map[string]bool
}

func OpenIDStore(path string) (*IDStore, error) {
	s := &IDStore{path: path, ids: map[string]bool{}}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if len(data) > 0 && data[len(data)-1] != '\n' {
			lastNewline := bytes.LastIndexByte(data, '\n')
			data = data[:lastNewline+1]
			if err := os.WriteFile(path, data, 0o600); err != nil {
				return nil, err
			}
		}
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			if len(line) == 0 {
				continue
			}
			var id string
			if err := json.Unmarshal(line, &id); err != nil {
				return nil, err
			}
			s.ids[id] = true
		}
	}
	return s, nil
}

func (s *IDStore) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ids[id]
}

func (s *IDStore) Mark(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[id] {
		return nil
	}
	data, err := json.Marshal(id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	s.ids[id] = true
	return nil
}

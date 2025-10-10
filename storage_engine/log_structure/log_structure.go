package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type LogStructureDB struct {
	filename string
	index    map[string]int64 // key -> file offset
	mu       sync.RWMutex
}

func NewLogStructuredDB(filename string) (*LogStructureDB, error) {
	db := &LogStructureDB{
		filename: filename,
		index:    make(map[string]int64),
	}

	if err := db.reabuildIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *LogStructureDB) reabuildIndex() error {
	file, err := os.Open(db.filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	defer file.Close()
	db.mu.Lock()         // get lock
	defer db.mu.Unlock() // defer로 바로 자원해제 예약

	var offset int64 = 0
	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')

		if err != nil && err != io.EOF {
			return err
		}

		if len(line) > 0 {
			trimmed := strings.TrimRight(line, "\n")
			parts := strings.SplitN(trimmed, ",", 2)

			if len(parts) == 2 {
				key := parts[0]
				db.index[key] = offset
			}
			offset += int64(len(line))
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

// Set

func (db *LogStructureDB) Set(key, value string) error {
	// 검증
	if strings.Contains(key, "\n") || strings.Contains(key, ",") {
		return fmt.Errorf("key contains invalid character")
	}

	record := fmt.Sprintf("%s,%s\n", key, value)

	db.mu.Lock()
	defer db.mu.Unlock()

	file, err := os.OpenFile(db.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// 파일

	defer func() {
		// 파일 닫을 때 발생한 오류 받기
		// 읽기 성공인데 닫기에서 에러난 경우에만 Close 에러를 돌려줌
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// 실제쓰기

	n, werr := file.WriteString(record)
	if werr != nil {
		return werr
	}
	if n != len(record) {
		return fmt.Errorf("short write: wrote %d, want %d", n, len(record))
	}

	// 디스크에 플러시 (내구성 필요시)
	if ferr := file.Sync(); ferr != nil {
		return ferr
	}

	// 쓰기 성공 후 인덱스 업데이트
	db.index[key] = offset
	return nil
}

func (db *LogStructureDB) Get(key string) (string, error) {
	db.mu.RLock()
	offset, exists := db.index[key]
	db.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("key not found")
	}

	file, err := os.Open(db.filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	trimmed := strings.TrimRight(line, "\n")
	parts := strings.SplitN(trimmed, ",", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("malformed record")
	}
	return parts[1], nil
}

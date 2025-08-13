package rotate

import (
	"os"
	"sort"
	"strconv"
	"strings"
)

func ScanAddIndexSuffixFiles(dirPath string, suffix string) []int64 {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil
	}

	list := make([]int64, 0, 8)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), suffix) {
			fname := entry.Name()
			fname = fname[:len(fname)-len(suffix)]
			idx, err := strconv.ParseInt(fname, 10, 64)
			if err != nil {
				continue
			}
			list = append(list, idx)
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i] < list[j]
	})
	return list
}

package redisModels

import (
	"bufio"
	"io"
	"log/slog"
	"strings"
)

func ParseMultilineToMap(name string, text string) (map[string]string, error) {
	dict := make(map[string]string, 16)
	reader := bufio.NewReader(strings.NewReader(text))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("Parse lines read line error", slog.String("name", name), slog.String("error", err.Error()), slog.String("text", text))
			return nil, err
		}

		// 去除换行符
		line = strings.Trim(line, "\n")
		line = strings.Trim(line, "\r")
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		index := strings.Index(line, ":")
		if index == -1 {
			slog.Warn("Parse cluster info read line format error", slog.String("name", name), slog.String("line", line))
			continue
		}
		section1 := line[:index]
		section2 := line[index+1:]
		section1 = strings.TrimSpace(section1)
		section2 = strings.TrimSpace(section2)
		dict[section1] = section2
	}
	return dict, nil
}

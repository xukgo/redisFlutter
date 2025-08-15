package http

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
)

type Pusher struct {
}

func uploadStreamAsGzip(url string, stream io.Reader, filename string) error {
	// 1. 创建一个管道（避免内存爆炸）
	pr, pw := io.Pipe()
	defer pr.Close()
	// 2. 创建 multipart writer
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// 3. 启动一个 goroutine 异步写入数据
	go func() {
		defer pw.Close()
		// 3.1 创建表单文件字段（注意文件名带 .gz 后缀）
		part, err := writer.CreateFormFile("file", filename+".gz")
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		// 3.2 将流数据压缩并写入表单
		gzipWriter := gzip.NewWriter(part)
		defer gzipWriter.Close()
		if _, err := io.Copy(gzipWriter, stream); err != nil {
			pw.CloseWithError(err)
			return
		}
	}()
	// 4. 完成 multipart 写入
	if err := writer.Close(); err != nil {
		return err
	}
	// 5. 发送 HTTP 请求
	resp, err := http.Post(url, writer.FormDataContentType(), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 6. 检查响应
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed: %s", resp.Status)
	}
	return nil
}
func (c *Pusher) UploadRdb() {
	// 1. 准备表单数据
	formData := map[string]string{
		"redisType":   "standalone", // 表单键值对
		"clusterSize": "3",
	}
	filePath := "/path/to/dump.rdb" // 要上传的文件路径
	// 2. 创建一个缓冲区用于构建 multipart 请求体
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// 3. 添加表单字段（键值对）
	for key, val := range formData {
		_ = writer.WriteField(key, val) // 写入文本字段
	}
	// 4. 添加文件字段
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	part, err := writer.CreateFormFile("file", "dump.rdb") // 字段名 + 文件名
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(part, file) // 将文件内容拷贝到 part
	if err != nil {
		panic(err)
	}
	// 5. 关闭 writer（必须！否则请求体会不完整）
	writer.Close()
	// 6. 发送 HTTP 请求
	req, err := http.NewRequest("POST", "http://localhost:8080/upload", body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType()) // 关键！设置 multipart 类型
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	// 7. 读取响应
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(respBody))
}

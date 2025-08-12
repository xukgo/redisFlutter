package main

import (
	"compress/gzip"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// # 普通文件上传
//
//	curl -X POST http://localhost:8080/upload \
//	 -F "filename=test.txt" \
//	 -F "file=@./original.txt"
//
// # GZIP压缩文件上传
//
//	curl -X POST http://localhost:8080/upload \
//	 -H "Content-Encoding: gzip" \
//	 -F "filename=compressed.txt" \
//	 -F "file=@./compressed.gz"
//
// FileUploadServer 文件上传服务器
type FileUploadServer struct {
	addr         string
	uploadDir    string
	maxChunkSize int64
	maxMemory    int64
	mu           sync.Mutex // 保护文件操作
}

// NewFileUploadServer 创建新的文件上传服务器实例
func NewFileUploadServer(addr string) *FileUploadServer {
	// 确保上传目录存在
	//if err := os.MkdirAll(uploadDir, 0755); err != nil {
	//	log.Fatalf("无法创建上传目录: %v", err)
	//}
	var maxChunkSize int64 = 32 * 1024
	var maxMemory int64 = 2 * 1024 * 1024

	return &FileUploadServer{
		addr:         addr,
		maxChunkSize: maxChunkSize,
		maxMemory:    maxMemory,
	}
}

// ServeHTTP 实现http.Handler接口
//func (s *FileUploadServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
//	switch r.URL.Path {
//	case "/upload":
//		s.handleUpload(w, r)
//	case "/health":
//		w.Write([]byte("OK"))
//	default:
//		http.NotFound(w, r)
//	}
//}

// handleUpload 处理文件上传请求
func (s *FileUploadServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "只支持POST方法", http.StatusMethodNotAllowed)
		return
	}
	// 限制内存使用
	if err := r.ParseMultipartForm(s.maxMemory); err != nil {
		http.Error(w, "解析表单错误: "+err.Error(), http.StatusBadRequest)
		return
	}
	// 获取文件名
	filename := r.FormValue("filename")
	if filename == "" {
		http.Error(w, "文件名不能为空", http.StatusBadRequest)
		return
	}
	// 安全处理文件名
	filename = filepath.Base(filename)
	filePath := filepath.Join(s.uploadDir, filename)
	// 获取上传的文件
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "获取文件错误: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	// 处理文件上传
	if err := s.processUpload(file, header, filePath); err != nil {
		http.Error(w, "处理上传失败: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("文件上传成功: " + filename))
}

// processUpload 处理文件上传逻辑
func (s *FileUploadServer) processUpload(file multipart.File, header *multipart.FileHeader, filePath string) error {
	// 加锁确保并发安全
	s.mu.Lock()
	defer s.mu.Unlock()

	// 创建目标文件
	output, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer output.Close()
	// 检查是否为gzip压缩
	contentEncoding := header.Header.Get("Content-Encoding")
	isGzipped := strings.Contains(strings.ToLower(contentEncoding), "gzip")
	var src io.Reader = file
	if isGzipped {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gz.Close()
		src = gz
	}
	// 使用缓冲区按chunk写入
	buf := make([]byte, s.maxChunkSize)
	_, err = io.CopyBuffer(output, src, buf)
	return err
}

// Start 启动HTTP服务器
func (s *FileUploadServer) Start() error {
	//log.Printf("文件上传服务器启动，监听 %s...", addr)
	//log.Printf("上传目录: %s", s.uploadDir)
	//log.Printf("配置: 块大小=%d字节, 最大内存=%d字节", s.maxChunkSize, s.maxMemory)
	// 创建ServeMux实例
	mux := http.NewServeMux()
	// 注册GET路由
	//mux.HandleFunc("GET /", s.getStatusHandler)
	//mux.HandleFunc("GET /about", aboutHandler)
	//mux.HandleFunc("GET /users/{id}", userHandler)

	// 注册POST路由
	//mux.HandleFunc("POST /login", loginHandler)
	mux.HandleFunc("POST /upload", s.handleUpload)

	// 启动服务器
	slog.Info("server start", slog.String("addr", s.addr))
	var err error
	if err = http.ListenAndServe(s.addr, mux); err != nil {
		slog.Error("server start error", slog.String("err", err.Error()))
	}
	return err
}

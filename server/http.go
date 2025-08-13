package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"redisFlutter/constDefine"
	rotate "redisFlutter/internal/utils/file_rotate"
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

	aofAddWriter *rotate.AofAddIndexWriter
}

// NewFileUploadServer 创建新的文件上传服务器实例
func NewFileUploadServer(addr string) *FileUploadServer {
	// 确保上传目录存在
	//if err := os.MkdirAll(uploadDir, 0755); err != nil {
	//	log.Fatalf("无法创建上传目录: %v", err)
	//}
	var maxChunkSize int64 = 32 * 1024
	var maxMemory int64 = 2 * 1024 * 1024

	c := &FileUploadServer{
		addr:         addr,
		maxChunkSize: maxChunkSize,
		maxMemory:    maxMemory,
		uploadDir:    "/tmp/httptest",
	}
	//var err error
	//c.aofAddWriter,err = rotate.NewAofAddIndexWriter("syncServer", path.Join(c.uploadDir, "default"), 8*1024*1024)
	return c
}

func (c *FileUploadServer) handleUploadAof(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only support POST method", http.StatusMethodNotAllowed)
		return
	}
	// 1. stream parse multipart form data
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// 2. save form data
	formMap := make(map[string]string)

	var filePart *multipart.Part = nil
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "Read Part Error", http.StatusInternalServerError)
			return
		}
		if part.FormName() != "" && part.FileName() == "" {
			valRaw, _ := io.ReadAll(part)
			valText := string(valRaw)
			formMap[part.FormName()] = valText
			continue
		}
		if part.FileName() != "" {
			filePart = part
			break
		}
	}

	redisTypeText, find := formMap["redisType"]
	if !find {
		http.Error(w, "redisType is empty", http.StatusBadGateway)
		return
	}
	if redisTypeText == "standalone" {
		c.processStandaloneAofUpload(w, formMap, filePart)
	} else if redisTypeText == "cluster" {
		//c.processClusterRdbUpload(w, r, filePart)
	} else {
		http.Error(w, "redisType is invalid", http.StatusBadGateway)
	}
}

// handleUpload 处理文件上传请求
func (c *FileUploadServer) handleUploadRdb(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only support POST method", http.StatusMethodNotAllowed)
		return
	}
	// 1. stream parse multipart form data
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// 2. save form data
	formMap := make(map[string]string)

	var filePart *multipart.Part = nil
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "Read Part Error", http.StatusInternalServerError)
			return
		}
		if part.FormName() != "" && part.FileName() == "" {
			valRaw, _ := io.ReadAll(part)
			valText := string(valRaw)
			formMap[part.FormName()] = valText
			continue
		}
		if part.FileName() != "" {
			filePart = part
			break
		}
	}

	redisTypeText, find := formMap["redisType"]
	if !find {
		http.Error(w, "redisType is empty", http.StatusBadGateway)
		return
	}
	if redisTypeText == "standalone" {
		c.processStandaloneRdbUpload(w, formMap, filePart)
	} else if redisTypeText == "cluster" {
		//c.processClusterRdbUpload(w, r, filePart)
	} else {
		http.Error(w, "redisType is invalid", http.StatusBadGateway)
	}
}

func (c *FileUploadServer) processStandaloneAofUpload(w http.ResponseWriter, formMap map[string]string, filePart *multipart.Part) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if filePart == nil {
		http.Error(w, "no file stream", http.StatusBadGateway)
		return
	}

	redisInstName, find := formMap["instance"]
	if !find {
		http.Error(w, "instance is empty", http.StatusBadGateway)
		return
	}
	aofIndex, find := formMap["aofIndex"]
	if !find {
		http.Error(w, "aofIndex is empty", http.StatusBadGateway)
		return
	}
	_ = aofIndex

	var err error
	saveDir := path.Join(c.uploadDir, redisInstName)
	err = os.MkdirAll(saveDir, 0755)
	if err != nil {
		http.Error(w, "Create Directory Error", http.StatusInternalServerError)
		return
	}
	err = c.saveAofStream(w, saveDir, filePart)
	if err != nil {
		return
	}
	w.Write([]byte("Upload Success"))
}

func (c *FileUploadServer) saveAofStream(w http.ResponseWriter, saveDir string, filePart *multipart.Part) error {
	return nil
}
func (c *FileUploadServer) processStandaloneRdbUpload(w http.ResponseWriter, formMap map[string]string, filePart *multipart.Part) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if filePart == nil {
		http.Error(w, "no file stream", http.StatusBadGateway)
		return
	}

	redisInstName, find := formMap["instance"]
	if !find {
		http.Error(w, "instance is empty", http.StatusBadGateway)
		return
	}

	var err error
	saveDir := path.Join(c.uploadDir, redisInstName)
	err = os.MkdirAll(saveDir, 0755)
	if err != nil {
		http.Error(w, "Create Directory Error", http.StatusInternalServerError)
		return
	}
	savePath := path.Join(saveDir, constDefine.REDIS_RDB_FILENAME)
	err = c.saveFile(w, savePath, filePart)
	if err != nil {
		return
	}
	w.Write([]byte("Upload Success"))
}

func (c *FileUploadServer) saveFile(w http.ResponseWriter, savePath string, filePart *multipart.Part) error {
	fi, err := os.Create(savePath)
	if err != nil {
		http.Error(w, "Create File Error", http.StatusInternalServerError)
		return err
	}
	defer fi.Close()

	aheadBuff := make([]byte, 2)
	n, err := filePart.Read(aheadBuff)
	if err != nil {
		return err
	}

	var reader io.Reader = filePart
	// push back Reader，avoid lost data
	if n > 0 {
		reader = io.MultiReader(bytes.NewReader(aheadBuff[:n]), filePart)
	}

	//check gzip magic number
	if n >= 2 && aheadBuff[0] == 0x1F && aheadBuff[1] == 0x8B {
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			http.Error(w, "Gzip decompress Error", http.StatusInternalServerError)
			return err
		}
		defer gzReader.Close()
		_, err = io.Copy(fi, gzReader)
		if err != nil {
			http.Error(w, "Write File Error", http.StatusInternalServerError)
			return err
		}
	} else {
		_, err := io.Copy(fi, reader)
		if err != nil {
			http.Error(w, "Write File Error", http.StatusInternalServerError)
			return err
		}
	}
	return nil
}

func (c *FileUploadServer) checkFormMapGzipEnable(formMap map[string]string) bool {
	redisInstName, find := formMap["isGzip"]
	if !find {
		return false
	}
	if strings.EqualFold(redisInstName, "true") {
		return true
	}
	return false
}

// Start 启动HTTP服务器
func (c *FileUploadServer) Start() error {
	//log.Printf("文件上传服务器启动，监听 %c...", addr)
	//log.Printf("上传目录: %c", c.uploadDir)
	//log.Printf("配置: 块大小=%d字节, 最大内存=%d字节", c.maxChunkSize, c.maxMemory)
	// 创建ServeMux实例
	mux := http.NewServeMux()
	// 注册GET路由
	//mux.HandleFunc("GET /", c.getStatusHandler)
	//mux.HandleFunc("GET /about", aboutHandler)
	//mux.HandleFunc("GET /users/{id}", userHandler)

	// 注册POST路由
	//mux.HandleFunc("POST /login", loginHandler)
	mux.HandleFunc("POST /uploadRdb", c.handleUploadRdb)
	mux.HandleFunc("POST /uploadAof", c.handleUploadAof)

	// 启动服务器
	slog.Info("server start", slog.String("addr", c.addr))
	var err error
	if err = http.ListenAndServe(c.addr, mux); err != nil {
		slog.Error("server start error", slog.String("err", err.Error()))
	}
	return err
}

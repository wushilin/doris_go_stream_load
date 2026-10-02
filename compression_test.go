package dorisstreamload

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/dsnet/compress/bzip2"
	"github.com/go-compressions/lzo"
	"github.com/pierrec/lz4/v4"
)

func TestCompressUploadBodyAllTypes(t *testing.T) {
	// Exceed LZOP's 256 KiB block size so the multi-block container is covered.
	plain := bytes.Repeat([]byte("1,alpha,some-repeated-stream-load-data\n"), 12000)
	tests := []struct {
		name string
		read func([]byte) ([]byte, error)
	}{
		{name: string(CompressionNone), read: func(data []byte) ([]byte, error) { return data, nil }},
		{name: string(CompressionGzip), read: func(data []byte) ([]byte, error) {
			r, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			defer r.Close()
			return io.ReadAll(r)
		}},
		{name: string(CompressionLZO), read: func(data []byte) ([]byte, error) {
			r, err := lzo.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			return io.ReadAll(r)
		}},
		{name: string(CompressionBZip2), read: func(data []byte) ([]byte, error) {
			r, err := bzip2.NewReader(bytes.NewReader(data), nil)
			if err != nil {
				return nil, err
			}
			return io.ReadAll(r)
		}},
		{name: string(CompressionLZ4), read: func(data []byte) ([]byte, error) { return io.ReadAll(lz4.NewReader(bytes.NewReader(data))) }},
		{name: string(CompressionLZOP), read: func(data []byte) ([]byte, error) {
			r, err := lzo.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			return io.ReadAll(r)
		}},
		{name: string(CompressionDeflate), read: func(data []byte) ([]byte, error) {
			r, err := zlib.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			defer r.Close()
			return io.ReadAll(r)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compressUploadBody(plain, CompressionType(tt.name))
			if err != nil {
				t.Fatalf("compressUploadBody() error = %v", err)
			}
			decoded, err := tt.read(compressed)
			if err != nil {
				t.Fatalf("decode compressed body: %v", err)
			}
			if !bytes.Equal(decoded, plain) {
				t.Fatalf("decoded body differs from source")
			}
		})
	}
}

func TestStreamLoadCompressionHeaderAndContentLength(t *testing.T) {
	for _, compressionType := range []CompressionType{
		CompressionNone, CompressionGzip, CompressionLZO, CompressionBZip2,
		CompressionLZ4, CompressionLZOP, CompressionDeflate,
	} {
		t.Run(string(compressionType), func(t *testing.T) {
			httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				if req.ContentLength != int64(len(body)) {
					t.Errorf("Content-Length = %d, body length = %d", req.ContentLength, len(body))
				}
				if compressionType == CompressionNone {
					if got := req.Header.Get("compress_type"); got != "" {
						t.Errorf("compress_type = %q, want absent", got)
					}
				} else if got := req.Header.Get("compress_type"); got != string(compressionType) {
					t.Errorf("compress_type = %q, want %q", got, compressionType)
				}
				payload, _ := json.Marshal(StreamLoadResponse{Status: "Success", Label: "test-label"})
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(payload))), Request: req}, nil
			})}

			s := &httpSender{cfg: Config{
				StreamLoadURL:   "http://example.invalid/api/db/table/_stream_load",
				Columns:         []string{"c1", "c2"},
				Mode:            ModeCSV,
				CompressionType: compressionType,
				HTTPClient:      httpClient,
			}}
			_, err := s.Send(context.Background(), &deliveryBatch{
				label:   "test-label",
				mode:    ModeCSV,
				csvRows: []string{"1,alpha"},
			})
			if err != nil {
				t.Fatalf("Send() error = %v", err)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

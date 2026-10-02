package dorisstreamload

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"time"

	"github.com/MediaMath/go-lzop/lzop"
	"github.com/dsnet/compress/bzip2"
	"github.com/pierrec/lz4/v4"
	"github.com/woozymasta/lzo"
)

func compressUploadBody(body []byte, compressionType CompressionType) ([]byte, error) {
	if compressionType == "" || compressionType == CompressionNone {
		return body, nil
	}

	var out bytes.Buffer
	switch compressionType {
	case CompressionGzip:
		writer := gzip.NewWriter(&out)
		if _, err := writer.Write(body); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	case CompressionDeflate:
		writer := zlib.NewWriter(&out)
		if _, err := writer.Write(body); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	case CompressionBZip2:
		writer, err := bzip2.NewWriter(&out, nil)
		if err != nil {
			return nil, err
		}
		if _, err := writer.Write(body); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	case CompressionLZ4:
		writer := lz4.NewWriter(&out)
		if _, err := writer.Write(body); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	case CompressionLZO, CompressionLZOP:
		compressed, err := lzop.CompressData(time.Now().Unix(), "batch.csv", body, func(src []byte) []byte {
			encoded, err := lzo.Compress(src, nil)
			if err != nil {
				return src
			}
			return encoded
		})
		if err != nil {
			return nil, err
		}
		return compressed, nil
	default:
		return nil, fmt.Errorf("unsupported compression type %q", compressionType)
	}
	return out.Bytes(), nil
}

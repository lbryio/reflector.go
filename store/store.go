package store

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"github.com/lbryio/errors.go"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type BlobStore interface {
	Has(string) (bool, error)
	Get(string) ([]byte, error)
	Put(string, []byte) error
}

type FileBlobStore struct {
	dir string

	initialized bool
}

func NewFileBlobStore(dir string) *FileBlobStore {
	return &FileBlobStore{dir: dir}
}

func (f *FileBlobStore) path(hash string) string {
	return path.Join(f.dir, hash)
}

func (f *FileBlobStore) initOnce() error {
	if f.initialized {
		return nil
	}
	defer func() { f.initialized = true }()

	if stat, err := os.Stat(f.dir); err != nil {
		if os.IsNotExist(err) {
			err2 := os.Mkdir(f.dir, 0755)
			if err2 != nil {
				return err2
			}
		} else {
			return err
		}
	} else if !stat.IsDir() {
		return errors.Err("blob dir exists but is not a dir")
	}
	return nil
}

func (f *FileBlobStore) Has(hash string) (bool, error) {
	err := f.initOnce()
	if err != nil {
		return false, err
	}

	_, err = os.Stat(f.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *FileBlobStore) Get(hash string) ([]byte, error) {
	err := f.initOnce()
	if err != nil {
		return []byte{}, err
	}

	file, err := os.Open(f.path(hash))
	if err != nil {
		return []byte{}, err
	}

	return ioutil.ReadAll(file)
}

func (f *FileBlobStore) Put(hash string, blob []byte) error {
	err := f.initOnce()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(f.path(hash), blob, 0644)
}

type S3BlobStore struct {
	awsID     string
	awsSecret string
	region    string
	bucket    string

	session *session.Session
}

func NewS3BlobStore(awsID, awsSecret, region, bucket string) *S3BlobStore {
	return &S3BlobStore{
		awsID:     awsID,
		awsSecret: awsSecret,
		region:    region,
		bucket:    bucket,
	}
}

func (s *S3BlobStore) initOnce() error {
	if s.session != nil {
		return nil
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(s.awsID, s.awsSecret, ""),
		Region:      aws.String(s.region),
	})
	if err != nil {
		return err
	}

	s.session = sess
	return nil
}

func (s *S3BlobStore) Has(hash string) (bool, error) {
	err := s.initOnce()
	if err != nil {
		return false, err
	}

	_, err = s3.New(s.session).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if reqFail, ok := err.(s3.RequestFailure); ok && reqFail.StatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *S3BlobStore) Get(hash string) ([]byte, error) {
	err := s.initOnce()
	if err != nil {
		return []byte{}, err
	}

	buf := &aws.WriteAtBuffer{}
	_, err = s3manager.NewDownloader(s.session).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		return buf.Bytes(), err
	}

	return buf.Bytes(), nil
}

func (s *S3BlobStore) Put(hash string, blob []byte) error {
	err := s.initOnce()
	if err != nil {
		return err
	}

	_, err = s3manager.NewUploader(s.session).Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
		Body:   bytes.NewBuffer(blob),
	})
	return err
}

package store

import (
	"bytes"
	"net/http"
	"time"

	"github.com/lbryio/lbry.go/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

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

	log.Debugf("Getting %s from S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", hash[:8], time.Since(t).String())
	}(time.Now())

	buf := &aws.WriteAtBuffer{}
	_, err = s3manager.NewDownloader(s.session).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return []byte{}, errors.Err("bucket %s does not exist", s.bucket)
			case s3.ErrCodeNoSuchKey:
				return []byte{}, errors.Err(ErrBlobNotFound)
			}
		}
		return buf.Bytes(), err
	}

	return buf.Bytes(), nil
}

func (s *S3BlobStore) Put(hash string, blob []byte) error {
	err := s.initOnce()
	if err != nil {
		return err
	}

	log.Debugf("Uploading %s to S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Uploading %s took %s", hash[:8], time.Since(t).String())
	}(time.Now())

	_, err = s3manager.NewUploader(s.session).Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
		Body:   bytes.NewBuffer(blob),
	})

	return err
}

func (s *S3BlobStore) PutSD(hash string, blob []byte) error {
	return s.Put(hash, blob)
}

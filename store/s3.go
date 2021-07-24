package store

import (
	"bytes"
	"net/http"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

// S3Store is an S3 store
type S3Store struct {
	awsID     string
	awsSecret string
	region    string
	bucket    string

	session *session.Session
}

// NewS3Store returns an initialized S3 store pointer.
func NewS3Store(awsID, awsSecret, region, bucket string) *S3Store {
	return &S3Store{
		awsID:     awsID,
		awsSecret: awsSecret,
		region:    region,
		bucket:    bucket,
	}
}

const nameS3 = "s3"

// Name is the cache type name
func (s *S3Store) Name() string { return nameS3 }

// Has returns T/F or Error ( from S3 ) if the store contains the blob.
func (s *S3Store) Has(hash string) (bool, error) {
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

// Get returns the blob slice if present or errors on S3.
func (s *S3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	//Todo-Need to handle error for blob doesn't exist for consistency.
	err := s.initOnce()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), err
	}

	log.Debugf("Getting %s from S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", hash[:8], time.Since(t).String())
	}(start)

	buf := &aws.WriteAtBuffer{}
	_, err = s3manager.NewDownloader(s.session).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("bucket %s does not exist", s.bucket)
			case s3.ErrCodeNoSuchKey:
				return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err(ErrBlobNotFound)
			}
		}
		return buf.Bytes(), shared.NewBlobTrace(time.Since(start), s.Name()), err
	}

	return buf.Bytes(), shared.NewBlobTrace(time.Since(start), s.Name()), nil
}

// Put stores the blob on S3 or errors if S3 connection errors.
func (s *S3Store) Put(hash string, blob stream.Blob) error {
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
		ACL:    aws.String("public-read"),
		//StorageClass: aws.String(s3.StorageClassIntelligentTiering),
	})
	metrics.MtrOutBytesReflector.Add(float64(blob.Size()))

	return err
}

// PutSD stores the sd blob on S3 or errors if S3 connection errors.
func (s *S3Store) PutSD(hash string, blob stream.Blob) error {
	//Todo - handle missing stream for consistency
	return s.Put(hash, blob)
}

func (s *S3Store) Delete(hash string) error {
	err := s.initOnce()
	if err != nil {
		return err
	}

	log.Debugf("Deleting %s from S3", hash[:8])

	_, err = s3.New(s.session).DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(hash),
	})

	return err
}

func (s *S3Store) initOnce() error {
	if s.session != nil {
		return nil
	}

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(s.awsID, s.awsSecret, ""),
		Region:      aws.String(s.region),
		Endpoint:    aws.String("https://s3.wasabisys.com"),
	})
	if err != nil {
		return err
	}

	s.session = sess
	return nil
}

// Shutdown shuts down the store gracefully
func (s *S3Store) Shutdown() {
	return
}

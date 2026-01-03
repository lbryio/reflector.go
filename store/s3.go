package store

import (
	"bytes"
	"context"
	stderrors "errors"
	"path"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// S3Store is an S3 store
type S3Store struct {
	client       *s3.Client
	awsID        string
	awsSecret    string
	region       string
	bucket       string
	endpoint     string
	name         string
	prefixLength int
}

type S3Params struct {
	Name         string `mapstructure:"name"`
	AwsID        string `mapstructure:"aws_id"`
	AwsSecret    string `mapstructure:"aws_secret"`
	Region       string `mapstructure:"region"`
	Bucket       string `mapstructure:"bucket"`
	Endpoint     string `mapstructure:"endpoint"`
	ShardingSize int    `mapstructure:"sharding_size"`
}

// NewS3Store returns an initialized S3 store pointer.
func NewS3Store(params S3Params) *S3Store {
	return &S3Store{
		awsID:        params.AwsID,
		awsSecret:    params.AwsSecret,
		region:       params.Region,
		bucket:       params.Bucket,
		endpoint:     params.Endpoint,
		name:         params.Name,
		prefixLength: params.ShardingSize,
	}
}

const nameS3 = "s3"

func S3StoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg S3Params
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewS3Store(cfg), nil
}

func init() {
	RegisterStore(nameS3, S3StoreFactory)
}

// Name is the cache type name
func (s *S3Store) Name() string { return nameS3 + "-" + s.name }

// Has returns T/F or Error (from S3) if the store contains the blob.
func (s *S3Store) Has(hash string) (bool, error) {
	err := s.initOnce()
	if err != nil {
		return false, err
	}

	ctx := context.Background()
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.shardedPath(hash)),
	})
	if err != nil {
		var notFound *types.NotFound
		var noSuchKey *types.NoSuchKey
		if stderrors.As(err, &notFound) || stderrors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, errors.Err(err)
	}

	return true, nil
}

// Get returns the blob slice if present or errors on S3.
func (s *S3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	err := s.initOnce()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), err
	}

	log.Debugf("Getting %s from S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Getting %s from %s took %s", hash[:8], s.Name(), time.Since(t).String())
	}(start)

	ctx := context.Background()
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err = manager.NewDownloader(s.client).Download(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.shardedPath(hash)),
	})
	if err != nil {
		var noSuchBucket *types.NoSuchBucket
		var noSuchKey *types.NoSuchKey
		var notFound *types.NotFound
		switch {
		case stderrors.As(err, &noSuchBucket):
			return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("bucket %s does not exist", s.bucket)
		case stderrors.As(err, &noSuchKey), stderrors.As(err, &notFound):
			return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err(ErrBlobNotFound)
		}
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err(err)
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

	ctx := context.Background()
	_, err = manager.NewUploader(s.client).Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.shardedPath(hash)),
		Body:   bytes.NewBuffer(blob),
		ACL:    types.ObjectCannedACLPublicRead,
	})
	if err != nil {
		return errors.Err(err)
	}
	metrics.MtrOutBytesReflector.Add(float64(blob.Size()))
	return nil
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

	ctx := context.Background()
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.shardedPath(hash)),
	})

	return errors.Err(err)
}

func (s *S3Store) initOnce() error {
	if s.client != nil {
		return nil
	}

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(s.region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.awsID, s.awsSecret, "")),
	)
	if err != nil {
		return errors.Err(err)
	}

	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s.endpoint)
		o.UsePathStyle = true
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return nil
}

func (s *S3Store) shardedPath(hash string) string {
	if s.prefixLength <= 0 || len(hash) < s.prefixLength {
		return hash
	}
	return path.Join(hash[:s.prefixLength], hash)
}

// Shutdown shuts down the store gracefully
func (s *S3Store) Shutdown() {
}

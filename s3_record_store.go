package gokvstore

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gustapinto/go-kv-store/gen"
	"google.golang.org/protobuf/proto"
)

// S3RecordStore A AWS S3 based record store. Implements [RecordStore]
type S3RecordStore struct {
	bucket string
	dir    string
	config aws.Config
	client *s3.Client
}

var _ RecordStore = (*S3RecordStore)(nil)

// NewS3RecordStore Create a new [S3RecordStore]
func NewS3RecordStore(bucket, dir string, config aws.Config) *S3RecordStore {
	client := s3.NewFromConfig(config)

	return &S3RecordStore{
		bucket: bucket,
		dir:    dir,
		config: config,
		client: client,
	}
}

func (s *S3RecordStore) List() ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.dir),
	})

	paths := make([]string, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			paths = append(paths, *obj.Key)
		}
	}

	return paths, nil
}

func (s *S3RecordStore) MakeRecordPath(fileId string) string {
	builder := strings.Builder{}
	builder.WriteString(s.dir)
	builder.WriteString("/")
	builder.WriteString(fileId)
	builder.WriteString(".binpb")

	return builder.String()
}

func (s *S3RecordStore) MakeStoreForCollection(dir string) (RecordStore, error) {
	builder := strings.Builder{}
	builder.WriteString(s.dir)
	builder.WriteString("/")
	builder.WriteString(dir)

	return NewS3RecordStore(s.bucket, builder.String(), s.config), nil
}

func (s *S3RecordStore) Read(recordPath string) (*gen.Record, error) {
	obj, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(recordPath),
	})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()

	buffer, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, err
	}

	var record gen.Record
	if err := proto.Unmarshal(buffer, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

func (s *S3RecordStore) Remove(recordPath string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(recordPath),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3RecordStore) Write(recordPath string, record *gen.Record) error {
	buffer, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(recordPath),
		Body:   bytes.NewBuffer(buffer),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3RecordStore) Truncate() error {
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return err
	}

	return nil
}

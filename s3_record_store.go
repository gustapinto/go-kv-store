package gokvstore

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gustapinto/go-kv-store/gen"
	"google.golang.org/protobuf/proto"
)

// s3RecordStore A AWS S3 based record store
type s3RecordStore struct {
	bucket    string
	dir       string
	accessKey string
	secretKey string
	region    string
	client    *s3.Client
}

var _ recordStore = (*s3RecordStore)(nil)

// NewS3RecordStore Create a new [s3RecordStore]
func NewS3RecordStore(bucket, dir, accessKey, secretKey, region string) *s3RecordStore {
	credentialsProvider := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	config, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(credentialsProvider),
		config.WithRegion(region),
	)
	if err != nil {
		return nil
	}

	client := s3.NewFromConfig(config)

	return &s3RecordStore{
		bucket:    bucket,
		dir:       dir,
		accessKey: accessKey,
		secretKey: secretKey,
		region:    region,
		client:    client,
	}
}

func (s *s3RecordStore) list() ([]string, error) {
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

func (s *s3RecordStore) makeRecordPath(fileId string) string {
	builder := strings.Builder{}
	builder.WriteString(s.dir)
	builder.WriteString("/")
	builder.WriteString(fileId)
	builder.WriteString(protobufBinaryExtension)

	return builder.String()
}

func (s *s3RecordStore) makeStoreForCollection(dir string) (recordStore, error) {
	builder := strings.Builder{}
	builder.WriteString(s.dir)
	builder.WriteString("/")
	builder.WriteString(dir)

	return NewS3RecordStore(s.bucket, builder.String(), s.accessKey, s.secretKey, s.region), nil
}

func (s *s3RecordStore) read(recordPath string) (*gen.Record, error) {
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

func (s *s3RecordStore) remove(recordPath string) error {
	panic("unimplemented")
}

func (s *s3RecordStore) write(recordPath string, record *gen.Record) error {
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

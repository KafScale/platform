package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type awsS3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type awsS3Client struct {
	bucket string
	region string
	api    awsS3API
	kmsKey string
}

// NewS3Client returns an AWS-backed S3 client.
func NewS3Client(ctx context.Context, cfg S3Config) (S3Client, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 bucket required")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 region required")
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	if cfg.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           cfg.Endpoint,
					PartitionID:   "aws",
					SigningRegion: cfg.Region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return newAWSClientWithAPI(cfg.Bucket, cfg.Region, cfg.KMSKeyARN, client), nil
}

func newAWSClientWithAPI(bucket, region, kmsKey string, api awsS3API) S3Client {
	return &awsS3Client{
		bucket: bucket,
		region: region,
		api:    api,
		kmsKey: kmsKey,
	}
}

func (c *awsS3Client) EnsureBucket(ctx context.Context) error {
	if err := c.headBucket(ctx); err == nil {
		return nil
	} else if !errors.Is(err, errBucketMissing) {
		return err
	}

	input := &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	}
	if cfg := c.bucketLocationConfig(); cfg != nil {
		input.CreateBucketConfiguration = cfg
	}
	_, err := c.api.CreateBucket(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
				return nil
			}
		}
		return fmt.Errorf("create bucket %s: %w", c.bucket, err)
	}
	return nil
}

var errBucketMissing = errors.New("bucket missing")

func (c *awsS3Client) headBucket(ctx context.Context) error {
	_, err := c.api.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchBucket" {
			return errBucketMissing
		}
	}
	return fmt.Errorf("head bucket %s: %w", c.bucket, err)
}

func (c *awsS3Client) bucketLocationConfig() *types.CreateBucketConfiguration {
	if c.region == "" || c.region == "us-east-1" {
		return nil
	}
	constraint := types.BucketLocationConstraint(c.region)
	return &types.CreateBucketConfiguration{LocationConstraint: constraint}
}

func (c *awsS3Client) UploadSegment(ctx context.Context, key string, body []byte) error {
	return c.putObject(ctx, key, body)
}

func (c *awsS3Client) UploadIndex(ctx context.Context, key string, body []byte) error {
	return c.putObject(ctx, key, body)
}

func (c *awsS3Client) putObject(ctx context.Context, key string, body []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}
	if c.kmsKey != "" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		input.SSEKMSKeyId = aws.String(c.kmsKey)
	}
	_, err := c.api.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

func (c *awsS3Client) DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}
	if header := rng.headerValue(); header != nil {
		input.Range = header
	}
	resp, err := c.api.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body %s: %w", key, err)
	}
	return data, nil
}

func (c *awsS3Client) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}
	resp, err := c.api.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body %s: %w", key, err)
	}
	return data, nil
}

func (c *awsS3Client) ListSegments(ctx context.Context, prefix string) ([]S3Object, error) {
	paginator := s3.NewListObjectsV2Paginator(c.api, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	out := make([]S3Object, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			out = append(out, S3Object{
				Key:  *obj.Key,
				Size: size,
			})
		}
	}
	return out, nil
}

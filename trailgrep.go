package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func client(region string) *s3.S3 {
	providers := []credentials.Provider{}
	providers = append(providers,
		&credentials.EnvProvider{},
		&credentials.SharedCredentialsProvider{},
		&ec2rolecreds.EC2RoleProvider{
			Client: ec2metadata.New(session.New()),
		})

	creds := credentials.NewChainCredentials(providers)

	awscfg := aws.NewConfig().
		WithCredentials(creds).
		WithRegion(region)

	return s3.New(session.New(awscfg))
}

func main() {
	var (
		region = flag.String("region", "eu-west-1", "AWS region to search in")
		prefix = flag.String("prefix", "", "Prefix to search under")
		n      = flag.Int("concurrency", 8, "How many concurrent keys to look at")
		usage  = func() {
			fmt.Fprintf(os.Stderr,
				"Usage of %s:\n\n  %s [opts] <bucket> <search term>\n\n",
				path.Base(os.Args[0]), path.Base(os.Args[0]))
			flag.PrintDefaults()
		}
	)

	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "please supply a bucket name and a search string\n")
		flag.Usage()
		os.Exit(1)
	}

	bucket := flag.Arg(0)
	target := []byte(flag.Arg(1))

	c := client(*region)

	keyC := keys(c, bucket, *prefix)

	wg := sync.WaitGroup{}

	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func() {
			grepFor(c, bucket, target, keyC)
			wg.Done()
		}()
	}

	wg.Wait()

}

func keys(s *s3.S3, bucket, prefix string) <-chan string {
	c := make(chan string)
	go func() {
		fn := func(p *s3.ListObjectsV2Output, last bool) (next bool) {
			if p == nil || p.Contents == nil {
				return true
			}

			for _, cont := range p.Contents {
				if cont == nil || cont.Key == nil {
					continue
				}
				c <- *cont.Key
			}

			if last {
				close(c)
			}

			return true
		}

		err := s.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		}, fn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not fetch keys from %s/%s: %s\n", bucket, prefix, err)
			os.Exit(1)
		}
	}()
	return c
}

func grepInKey(s *s3.S3, bucket, key string, target []byte, buf *bytes.Buffer) bool {
	out, err := s.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("Could not get %s: %s", key, err)
		return false
	}
	defer out.Body.Close()

	_, err = buf.ReadFrom(out.Body)
	if err != nil {
		log.Printf("Could not from s3 %s: %s", key, err)
		return false
	}

	return bytes.Contains(buf.Bytes(), target)

}

func grepFor(s *s3.S3, bucket string, target []byte, keys <-chan string) {
	buf := bytes.Buffer{}

	for k := range keys {
		buf.Reset()
		if grepInKey(s, bucket, k, target, &buf) {
			f, err := os.Create(path.Base(k))
			if err != nil {
				log.Printf("Could not open output file %s: %s", path.Base(k), err)
				continue
			}
			_, err = io.Copy(f, &buf)
			f.Close()
			if err != nil {
				log.Printf("Could not write to %s: %s", path.Base(k), err)
			}
		}
	}
}

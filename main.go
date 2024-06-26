package main

import (
	"context"
	"crypto/x509"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var (
	endpoint   = ""
	bucketName = ""
	username   = ""
	password   = ""
	ca         = ""
)

type User struct {
	Name      string   `json:"name"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
	Random    string   `json:"random"`
}

var bigString = ""

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand = rand.New(
		rand.NewSource(time.Now().UnixNano())) // Seed for better randomness

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
func writer(ctx context.Context, col *gocb.Collection) error {
	t := time.NewTicker(time.Millisecond * 500)
	defer t.Stop()
	for {
		select {
		case <-t.C:

			id := uuid.New().String()
			user := User{
				Name:      id,
				Email:     id,
				Interests: []string{"Holy Grail", "African Swallows"},
				Random:    bigString,
			}

			_, err := col.Upsert(id, user, &gocb.UpsertOptions{
				RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
			})

			if err != nil {
				if strings.Contains(err.Error(), "KV_TEMPORARY_FAILURE") {
					fmt.Println("Temporary Error")
					if err, ok := (err).(gocb.RetryReason); ok {
						fmt.Printf("%+v\n", err)
						if err.Description() == "KV_TEMPORARY_FAILURE" {
							break
						}
					}
					break
				}
				return err
			}

			fmt.Println("Doc send : ", id)
		case <-ctx.Done():
			return nil

		}
	}
}

func main() {

	flag.StringVar(&endpoint, "endpoint", "", "Capella Cluster A Record")
	flag.StringVar(&bucketName, "bucket", "travel-sample", "Bucket name of Capella Cluster")
	flag.StringVar(&username, "username", "admin", "Username for Database Access Credentials")
	flag.StringVar(&password, "password", "admin", "Password for Database Access Credentials")
	flag.StringVar(&ca, "ca", "", "Signing Authority Certificate")
	flag.Parse()

	numWrite := 30
	bigString = generateRandomString(10000000)

	eg, ctx := errgroup.WithContext(context.Background())

	cp := x509.NewCertPool()
	if ok := cp.AppendCertsFromPEM([]byte(ca)); !ok {
		panic("failed to pass ca cert")
	}
	sec := gocb.SecurityConfig{
		TLSRootCAs: cp,
	}

	cluster, err := gocb.Connect("couchbases://"+endpoint, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: 10 * time.Second,
			KVTimeout:      20 * time.Second,
		},
		SecurityConfig: sec,
	})
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	col := bucket.Scope("_default").Collection("_default")
	for k := 0; k < numWrite; k++ {
		eg.Go(func() error {
			return writer(ctx, col)
		})
	}

	if err := eg.Wait(); err != nil {
		panic(err)
	}
}

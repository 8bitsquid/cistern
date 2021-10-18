package restic

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

const (
	ENV_VAR_ACCESS_KEY = "AWS_ACCESS_KEY_ID"
	ENV_VAR_SECRET     = "AWS_SECRET_ACCESS_KEY"
)

type S3BasicAuth struct {
	AccessKey string `mapstructure:"access_key"`
	Secret    string
}

type S3Config struct {
	URL            string
	BucketPath     string `mapstructure:"bucket_path"`
	Namespace      string
	ResticPassword string      `mapstructure:"restic_password"`
	Auth           S3BasicAuth `mapstructure:",squash"`
}

type S3 struct {
	config *S3Config
	Repo   string
}

func NewS3(config *S3Config) (*S3, error) {
	os.Setenv(ENV_VAR_ACCESS_KEY, config.Auth.AccessKey)
	os.Setenv(ENV_VAR_SECRET, config.Auth.Secret)
	os.Setenv(ENV_VAR_RESITIC_PASSWORD, config.ResticPassword)

	repo := buildRepoPath(config.URL, config.BucketPath)

	if exists := RepoExists(repo); !exists {
		resp, err := CreateRepo(repo)
		if err != nil {
			return nil, err
		}
		zap.S().Infof("Repo Created: %+v", resp)
	}

	s3 := &S3{
		config: config,
		Repo:   repo,
	}

	os.Setenv(ENV_VAR_RESTIC_REPOSITORY, repo)

	return s3, nil
}

func (s3 *S3) InitRepo() error {

	if err := s3.Snapshots(); err != nil {
		resp, err := RunResticCmd(CMD_INIT)
		if err != nil {
			return err
		}
		zap.S().Infof("Restic Init new repo created", resp)
	}

	zap.S().Infof("Restic repo exists", os.Getenv(ENV_VAR_RESTIC_REPOSITORY))

	return nil
}

func (s3 *S3) RunCmd(cmdArgs ...string) (interface{}, error) {
	args := []string{"-r", s3.Repo}
	args = append(args, cmdArgs...)
	resp, err := RunResticCmd(args...)
	zap.S().Debugw("S3 Restic Response", "resp", resp)
	return resp, err
}

func (s3 *S3) Snapshots() error {
	_, err := s3.RunCmd(CMD_SNAPSHOTS)
	if err != nil {
		return err
	}
	
	return nil
}

func buildRepoPath(url string, path string) string {
	return fmt.Sprintf("s3:%s/%s", url, path)
}

package restic

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"

	"github.com/facebookarchive/runcmd"
	"gitlab.com/heb-engineering/teams/spm-eng/appcloud/tools/salesforce-backups/tools"
	"go.uber.org/zap"
)

const (

	// Base restic command
	CMD = "restic"
	// Global command args
	CMD_ARG_CERIFICATES       = "--cacert"
	CMD_ARG_CACHE_DIR         = "--cache-dir"
	CMD_ARG_CACHE_CLEAN       = "--cleanup-cache"
	CMD_ARG_CACHE_DISABLED    = "--no-cache"
	CMD_ARG_OUTPUT_JSON       = "--json"
	CMD_ARG_OPTION            = "--option"
	CMD_ARG_LIMIT_DOWNLOAD    = "--limit-download"
	CMD_ARG_LIMIT_UPLOAD      = "--limit-upload"
	CMD_ARG_REPO              = "--repo"
	CMF_ARG_REPO_FILE         = "--repository-file"
	CMD_ARG_REPO_CERT         = "--tls-client-cert"
	CMD_ARG_REPO_LOC_DISABLED = "--no-lock"
	CMD_ARG_TAG               = "--tag"
	CMD_ARG_VERBOSE           = "--verbose"

	// TODO: add sub-command arg constants as needed
	CMD_INIT      = "init"
	CMD_BACKUP    = "backup"
	CMD_SNAPSHOTS = "snapshots"

	ENV_VAR_RESITIC_PASSWORD  = "RESTIC_PASSWORD"
	ENV_VAR_RESTIC_REPOSITORY = "RESTIC_REPOSITORY"
)

type Repo interface {
	InitRepo() error
	RunCmd(...string) (interface{}, error)
}

func RunResticCmd(cmdArgs ...string) (interface{}, error) {

	if os.Getenv(ENV_VAR_RESTIC_REPOSITORY) != "" && !tools.StringSliceContaines(cmdArgs, "-r") {
		return nil, errors.New("no Resitc repository defined, use the '-r' arg or define env variable")
	}

	// if one string, slit args into slice
	if len(cmdArgs) == 1 {
		cmdArgs = strings.Split(cmdArgs[0], " ")
	}

	// Append output format flag
	cmdArgs = append(cmdArgs, CMD_ARG_OUTPUT_JSON)

	// Tun and capture output
	cmd := exec.Command(CMD, cmdArgs...)

	streams, err := runcmd.Run(cmd)
	if err != nil {
		zap.S().Errorw("unable to run Restic command", "error", err.Error())
	}

	stdout := new(interface{})
	json.Unmarshal(streams.Stdout().Bytes(), stdout)

	zap.S().Debugw("Restic command done", "output", stdout, "error", err)

	return stdout, err
}

func RepoExists(repo string) bool {
	zap.S().Infof("Checking if repo exists: %+v", repo)
	out, err := RunResticCmd(CMD_ARG_REPO, repo, CMD_SNAPSHOTS)
	zap.S().Debugw("repo exists check", "output", out, "error", err)
	return err == nil
}

func CreateRepo(repo string) (interface{}, error) {
	zap.S().Infof("Attempting to create repo: %+v", repo)
	return RunResticCmd(CMD_ARG_REPO, repo, CMD_INIT)
}

func Backup(filePath string) (interface{}, error) {
	return RunResticCmd(CMD_BACKUP, filePath)
}

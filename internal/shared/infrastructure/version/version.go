package version

import "fmt"

var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

func Info() string {
	return fmt.Sprintf("DispatchHub %s (commit: %s, built: %s)", Version, GitCommit, BuildDate)
}

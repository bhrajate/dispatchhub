package signals

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/dispatchhub/dispatchhub/pkg/log"
)

// SetupSignalContext 返回在收到 SIGINT 或 SIGTERM 时取消的 context。
// 第二次收到信号会强制立即退出。
func SetupSignalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-ch
		log.Infof("received signal %s, initiating graceful shutdown", sig)
		cancel()

		sig = <-ch
		log.Infof("received second signal %s, forcing exit", sig)
		os.Exit(1)
	}()

	return ctx
}

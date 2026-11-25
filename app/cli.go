package app

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"time"

	"orchestrator/common"
	"orchestrator/metadata"

	"gopkg.in/urfave/cli.v1"
)

var (
	app                 = cli.NewApp()
	nodeManager         *Manager
	pprofListenAndServe = http.ListenAndServe
	onPprofError        = func(address string, err error) {
		common.GlobalLogger.Errorf("pprof server failed on %s: %v", address, err)
	}
)

func Run() {
	err := app.Run(os.Args)
	if err != nil {
		// Errors returned here bypass the zap redacting core, so scrub them
		// before they reach stderr and whatever captures it.
		fmt.Fprintln(os.Stderr, common.LogRedactor.Redact(err.Error()))
		os.Exit(1)
	}
}

func Stop() {
	err := nodeManager.Stop()
	if err != nil {
		panic(common.LogRedactor.Redact(err.Error()))
	}
}

func init() {
	app.Name = filepath.Base(os.Args[0])
	app.HideVersion = false
	app.Version = metadata.Version
	app.Compiled = time.Now()
	app.Usage = "orchestrator Node"
	app.Commands = []cli.Command{
		versionCommand,
	}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = AllFlags
	app.Before = beforeAction
	app.Action = action
	app.After = afterAction
}
func beforeAction(ctx *cli.Context) error {

	max := runtime.NumCPU()
	fmt.Printf("Starting orchestrator.\n")
	fmt.Printf("current time is %v\n", time.Now().Format("2009-01-03 18:15:05"))
	fmt.Printf("version: %v\n", metadata.Version)
	fmt.Printf("git-commit-hash: %v\n", metadata.GitCommit)
	fmt.Printf("orchestrator will use at most %v cpu-cores\n", max)
	runtime.GOMAXPROCS(max)

	// pprof server
	if ctx.GlobalBool(PprofFlag.Name) {
		listenHost := ctx.GlobalString(PprofAddrFlag.Name)
		port := ctx.GlobalUint64(PprofPortFlag.Name)
		if port == 0 || port > 65535 {
			return fmt.Errorf("invalid pprof port %d: must be between 1 and 65535", port)
		}

		address := net.JoinHostPort(listenHost, strconv.FormatUint(port, 10))

		go func() {
			if err := pprofListenAndServe(address, nil); err != nil {
				onPprofError(address, err)
			}
		}()
	}

	return nil
}
func action(ctx *cli.Context) error {
	if args := ctx.Args(); len(args) > 0 {
		return fmt.Errorf("invalid command: %q", args[0])
	}
	var err error
	nodeManager, err = NewNodeManager(ctx)
	if err != nil {
		return err
	}

	return nodeManager.Start()

}
func afterAction(*cli.Context) error {
	return nil
}

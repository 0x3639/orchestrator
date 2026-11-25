package app

import (
	"go.uber.org/zap"
	"gopkg.in/urfave/cli.v1"
	"orchestrator/common"
	"orchestrator/node"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Manager struct {
	ctx      *cli.Context
	node     *node.Node
	logger   *zap.Logger
	stopOnce sync.Once
	stopErr  error
}

func NewNodeManager(ctx *cli.Context) (*Manager, error) {
	// make config
	nodeConfig, err := MakeConfig()
	if err != nil {
		return nil, err
	}
	nodeConfig.EvmBackfillBlocks = ctx.GlobalUint64(EvmBackfillBlocksFlag.Name)
	nodeConfig.AcknowledgedRedeems = ctx.GlobalStringSlice(AcknowledgeRedeemFlag.Name)

	logger, err := common.CreateLogger()
	if err != nil {
		return nil, err
	}

	// make node
	newNode, err := node.NewNode(nodeConfig, logger)

	if err != nil {
		logger.Info("failed to create the node", zap.String("reason", err.Error()))
		return nil, err
	}

	return &Manager{
		ctx:    ctx,
		node:   newNode,
		logger: logger,
	}, nil
}

func (nodeManager *Manager) Start() error {
	// Start up the node
	nodeManager.logger.Info("starting orchestrator")
	if err := nodeManager.node.Start(); err != nil {
		nodeManager.logger.Fatal("failed to start node", zap.String("reason", err.Error()))
		os.Exit(1)
	} else {
		nodeManager.logger.Info("orchestrator successfully started")
	}

	// Listening event closes the node
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		defer signal.Stop(c)
		<-c
		nodeManager.logger.Info("Shutting down orchestrator from go func")

		go func() {
			if err := nodeManager.Stop(); err != nil {
				nodeManager.logger.Error(err.Error())
			}
		}()

		for i := 10; i > 0; i-- {
			<-c
			if i > 1 {
				nodeManager.logger.Warn("Please DO NOT interrupt the shutdown process, panic may occur.", zap.String("times", string(rune(i-1))))
			}
		}
	}()

	// Waiting for node to close. Wait can return on an internal stop signal
	// while a shutdown started by an OS signal is still running, so always
	// call Stop: it runs once, and a second caller waits for the first.
	nodeManager.node.Wait()
	// An incomplete shutdown makes the process exit non-zero.
	return nodeManager.Stop()
}

// Stop saves the config and stops the node once. Concurrent and later calls
// wait for the first one to finish and return its node.Stop error.
func (nodeManager *Manager) Stop() error {
	nodeManager.stopOnce.Do(func() { nodeManager.stopErr = nodeManager.stop() })
	return nodeManager.stopErr
}

func (nodeManager *Manager) stop() error {
	nodeManager.logger.Warn("Stopping orchestrator ...")

	if err := nodeManager.SaveConfig(); err != nil {
		nodeManager.logger.Info("Failed to save config", zap.String("reason", err.Error()))
	}

	err := nodeManager.node.Stop()
	if err != nil {
		nodeManager.logger.Info("Failed to stop node", zap.String("reason", err.Error()))
	} else {
		nodeManager.logger.Info("successfully stopped node")
	}
	return err
}

func (nodeManager *Manager) SaveConfig() error {
	nodeManager.logger.Info("Write config to file")
	return nodeManager.node.SaveConfig()
}

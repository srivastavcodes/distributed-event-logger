package main

import (
	"errors"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/srivastavcodes/distributed-event-logger/internal/agent"
	"github.com/srivastavcodes/distributed-event-logger/internal/config"
)

func main() {
	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "cubelog",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}
	if err := setupFlags(cmd); err != nil {
		log.Fatal().Err(err)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err)
	}
}

type cli struct {
	config cliConfig
}

type cliConfig struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cmd.Flags().String("config-file", "", "Path to config file.")

	dataDir := path.Join(os.TempDir(), "cubelog")
	cmd.Flags().String("data-dir", dataDir, "Directory to store log and raft data.")

	cmd.Flags().String("node-name", hostname, "Unique server ID.")

	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and raft) connections.")

	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster")

	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")

	cmd.Flags().String("client-tls-cert-file", "", "Path to client tls cert.")
	cmd.Flags().String("client-tls-key-file", "", "Path to client tls key.")
	cmd.Flags().String("client-tls-ca-file", "", "Path to client certificate authority.")

	return viper.BindPFlags(cmd.Flags())
}

// setupConfig reads the configuration and prepares the agent's configuration.
// Cobra calls setupConfig before running the commands RunE function.
func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigFile(configFile)
	if err = viper.ReadInConfig(); err != nil {
		// it's fine if config file doesn't exist
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return err
		}
	}
	c.config.DataDir = viper.GetString("data-dir")

	c.config.NodeName = viper.GetString("node-name")
	c.config.BindAddr = viper.GetString("bind-addr")
	c.config.RPCPort = viper.GetInt("rpc-port")

	c.config.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.config.Bootstrap = viper.GetBool("bootstrap")

	c.config.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.config.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.config.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")

	c.config.PeerTLSConfig.CertFile = viper.GetString("client-tls-cert-file")
	c.config.PeerTLSConfig.KeyFile = viper.GetString("client-tls-key-file")
	c.config.PeerTLSConfig.CAFile = viper.GetString("client-tls-ca-file")

	if c.config.ServerTLSConfig.CertFile != "" && c.config.ServerTLSConfig.KeyFile != "" {
		c.config.ServerTLSConfig.Server = true
		c.config.Config.ServerTLSConfig, err = config.SetupTLSConfig(c.config.ServerTLSConfig)
		if err != nil {
			return err
		}
	}
	if c.config.PeerTLSConfig.CertFile != "" && c.config.PeerTLSConfig.KeyFile != "" {
		c.config.Config.PeerTLSConfig, err = config.SetupTLSConfig(c.config.PeerTLSConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

// run runs our executable logic by:
//   - creating the agent.
//   - handling signals from the operating system.
//   - shutting down the agent gracefully when the operating system terminates the program
func (c *cli) run(cmd *cobra.Command, args []string) error {
	agnt, err := agent.NewAgent(c.config.Config)
	if err != nil {
		return err
	}
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	return agnt.Shutdown()
}

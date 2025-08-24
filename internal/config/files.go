package config

import (
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

var _ = godotenv.Load("../../.envrc")

var (
	CAFile        = configFile("ca.pem")
	ACLModelFile  = configFile("model.conf")
	ACLPolicyFile = configFile("policy.csv")

	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")

	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		cd := "../../"
		return filepath.Join(cd, dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}

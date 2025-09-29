package config

import (
	"github.com/joho/godotenv"
	"os"
	"path/filepath"
)

var _ = godotenv.Load("../../.envrc")

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root.pem")
	RootClientKeyFile    = configFile("root-key.pem")
	NobodyClientCertFile = configFile("nobody.pem")
	NobodyClientKeyFile  = configFile("nobody-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

func configFile(filename string) string {
	dir := os.Getenv("CONFIG_DIR")
	if dir != "" {
		return filepath.Join(dir, filename)
	}
	homedir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homedir, ".proglog", filename)
}

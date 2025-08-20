package main

import (
	"Proglog/internal/server"
	"log"
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	svr := server.NewHTTPServer(":7714")
	logger.Info("starting server on port 7714")

	log.Fatal(svr.ListenAndServe())
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"debtster_import/internal/config"
	"debtster_import/internal/handlers"
	"debtster_import/internal/server"
)

func main() {
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := config.Init(setupCtx)
	fmt.Println("✅ All connections successfully established!")

	if err := cfg.CheckConnections(setupCtx); err != nil {
		log.Fatalf("❌ Connection check failed: %v", err)
	}
	fmt.Println("🟢 All connections OK")

	h := handlers.New(cfg.Postgres, cfg.Mongo, cfg.S3)
	srv := server.NewServer(cfg.Port, h)

	if err := srv.Run(runCtx); err != nil {
		log.Fatal(err)
	}
}

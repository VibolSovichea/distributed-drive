package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/VibolSovichea/distributed-drive/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	if err := app.Run(ctx); err != nil {
		stop()
		fmt.Fprintf(os.Stderr, "distributed-drive: %v\n", err)
		os.Exit(1)
	}
}

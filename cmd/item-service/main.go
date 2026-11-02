// Command item-service runs the Item Service gRPC server.
package main

import (
	"database/sql"
	"log/slog"
	"net"
	"os"

	_ "github.com/go-sql-driver/mysql" // Registers the "mysql" driver with database/sql.
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"go_services/internal/item"
	"go_services/internal/itempb"
)

const (
	grpcAddr  = ":50061"
	mysqlDSN  = "app:app@tcp(localhost:3306)/shop"
	redisAddr = "localhost:6379"
)

func main() {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		fatal("open mysql", err)
	}
	// sql.Open does not connect, so check the connection now.
	if err := db.Ping(); err != nil {
		fatal("connect to mysql", err)
	}

	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		fatal("listen", err)
	}

	server := grpc.NewServer()
	itempb.RegisterItemServiceServer(server, &item.Server{
		DB:    db,
		Redis: redis.NewClient(&redis.Options{Addr: redisAddr}),
	})

	slog.Info("item-service listening", "addr", grpcAddr)
	if err := server.Serve(listener); err != nil {
		fatal("serve", err)
	}
}

func fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}

package util

import (
	"os"
	"os/signal"
	"syscall"
)

func SetupSignalHandler() (stopCh <-chan struct{}) {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, []os.Signal{os.Interrupt, syscall.SIGTERM}...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1)
	}()
	return stop
}

package raft

import (
	"log"
	"sync"
)

// LeaderService is implemented by services that should run only on the leader
// (e.g., TTL sweeper, touch flusher).
type LeaderService interface {
	Start()
	Stop()
}

// LeaderMonitor watches Raft leadership changes and starts/stops
// leader-only services accordingly.
type LeaderMonitor struct {
	services []LeaderService
	leaderCh <-chan bool
	stopCh   chan struct{}
	doneCh   chan struct{}
	mu       sync.Mutex
	leading  bool
}

// NewLeaderMonitor creates a monitor that watches the given leadership channel.
func NewLeaderMonitor(leaderCh <-chan bool, services ...LeaderService) *LeaderMonitor {
	return &LeaderMonitor{
		services: services,
		leaderCh: leaderCh,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Run watches for leadership transitions. Call from a goroutine.
func (m *LeaderMonitor) Run() {
	defer close(m.doneCh)
	for {
		select {
		case isLeader, ok := <-m.leaderCh:
			if !ok {
				m.stopServices()
				return
			}
			if isLeader {
				m.startServices()
			} else {
				m.stopServices()
			}
		case <-m.stopCh:
			m.stopServices()
			return
		}
	}
}

// Stop signals the monitor to shut down and waits.
func (m *LeaderMonitor) Stop() {
	close(m.stopCh)
	<-m.doneCh
}

func (m *LeaderMonitor) startServices() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leading {
		return
	}
	m.leading = true
	log.Printf("raft: became leader, starting %d services", len(m.services))
	for _, svc := range m.services {
		svc.Start()
	}
}

func (m *LeaderMonitor) stopServices() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.leading {
		return
	}
	m.leading = false
	log.Printf("raft: lost leadership, stopping %d services", len(m.services))
	for _, svc := range m.services {
		svc.Stop()
	}
}

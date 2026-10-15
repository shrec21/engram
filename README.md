# Engram

A replicated memory store for AI agents. Week 1: single-node with
linearizable CAS, TTL expiry, and MCP integration.

## Quickstart

```bash
# Build
make build

# Start the server
./engramd --data ./engram-data --addr :50051

# In another terminal:
# Write a memory
./engramctl put agents task-status '{"status":"running"}' --created-by planner

# Read it back
./engramctl get agents task-status

# Compare-and-set (version must match)
./engramctl cas agents task-status '{"status":"done"}' --version 1

# List all memories in a namespace
./engramctl list agents

# Delete
./engramctl delete agents task-status

# Write with TTL (auto-expires)
./engramctl put agents temp-note "remember this" --ttl 5m
```

## MCP Integration

Start the MCP stdio server (connects to a running engramd):

```bash
./engramctl mcp
```

Tools exposed: `memory_get`, `memory_put`, `memory_cas`, `memory_delete`, `memory_list`.

## Tests

```bash
make test
```

## Architecture

See [docs/DESIGN.md](docs/DESIGN.md) for the Apply constraint and CAS rationale.

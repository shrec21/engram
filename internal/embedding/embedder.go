package embedding

import "context"

// Embedder produces vector embeddings from text. Never called inside Apply.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dimension() int
	Model() string
}

package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// OllamaEmbedder calls Ollama's /api/embed endpoint.
type OllamaEmbedder struct {
	url    string // e.g. "http://localhost:11434"
	model  string // e.g. "nomic-embed-text"
	dim    int
	client *http.Client
}

// NewOllamaEmbedder creates an embedder that calls Ollama.
func NewOllamaEmbedder(url, model string, dim int) *OllamaEmbedder {
	return &OllamaEmbedder{
		url:    url,
		model:  model,
		dim:    dim,
		client: &http.Client{},
	}
}

type ollamaEmbedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type ollamaEmbedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

func (o *OllamaEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(ollamaEmbedRequest{
		Model: o.model,
		Input: texts,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", o.url+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result ollamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Embeddings) != len(texts) {
		return nil, fmt.Errorf("expected %d embeddings, got %d", len(texts), len(result.Embeddings))
	}

	return result.Embeddings, nil
}

func (o *OllamaEmbedder) Dimension() int { return o.dim }
func (o *OllamaEmbedder) Model() string  { return o.model }

var _ Embedder = (*OllamaEmbedder)(nil)

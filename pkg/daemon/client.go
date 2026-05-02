package daemon

import (
	"P2P-CDN/pkg/protocol"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Seed(filePath, fallbackURL string) (string, error) {
	return c.SeedWithTracker(filePath, fallbackURL, "")
}

func (c *Client) SeedWithTracker(filePath, fallbackURL, trackerURL string) (string, error) {
	req := protocol.DaemonSeedRequest{
		FilePath:    filePath,
		FallbackURL: fallbackURL,
		TrackerURL:  trackerURL,
	}

	var resp protocol.DaemonSeedResponse
	if err := c.post("/api/v1/seed", req, &resp); err != nil {
		return "", err
	}

	if !resp.Success {
		return "", fmt.Errorf("seed failed: %s", resp.Message)
	}

	return resp.FileHash, nil
}

func (c *Client) Unseed(fileHash string) error {
	endpoint := fmt.Sprintf("/api/v1/seed/%s", fileHash)

	req, err := http.NewRequest(http.MethodDelete, c.baseURL+endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unseed failed (status %d): %s", resp.StatusCode, string(body))
	}

	var unseedResp protocol.DaemonUnseedResponse
	if err := json.NewDecoder(resp.Body).Decode(&unseedResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !unseedResp.Success {
		return fmt.Errorf("unseed failed: %s", unseedResp.Message)
	}

	return nil
}

func (c *Client) Download(fileHash, outputPath string) error {
	return c.DownloadWithTracker(fileHash, outputPath, "")
}

func (c *Client) DownloadWithTracker(fileHash, outputPath, trackerURL string) error {
	req := protocol.DaemonDownloadRequest{
		FileHash:   fileHash,
		Output:     outputPath,
		TrackerURL: trackerURL,
	}

	var resp protocol.DaemonDownloadResponse
	if err := c.post("/api/v1/download", req, &resp); err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("download failed: %s", resp.Message)
	}

	return nil
}

func (c *Client) GetStatus(fileHash string) (*protocol.DaemonStatusResponse, error) {
	endpoint := fmt.Sprintf("/api/v1/status/%s", fileHash)

	var status protocol.DaemonStatusResponse
	if err := c.get(endpoint, &status); err != nil {
		return nil, err
	}

	return &status, nil
}

func (c *Client) GetInfo() (*protocol.DaemonInfoResponse, error) {
	var info protocol.DaemonInfoResponse
	if err := c.get("/api/v1/info", &info); err != nil {
		return nil, err
	}

	return &info, nil
}

func (c *Client) ListSeeds() (*protocol.DaemonListSeedsResponse, error) {
	var seeds protocol.DaemonListSeedsResponse
	if err := c.get("/api/v1/seeding", &seeds); err != nil {
		return nil, err
	}

	return &seeds, nil
}

func (c *Client) ListDownloads() (*protocol.DaemonListDownloadsResponse, error) {
	var downloads protocol.DaemonListDownloadsResponse
	if err := c.get("/api/v1/downloads", &downloads); err != nil {
		return nil, err
	}

	return &downloads, nil
}

func (c *Client) ListPeers() (*protocol.DaemonPeersResponse, error) {
	var peers protocol.DaemonPeersResponse
	if err := c.get("/api/v1/peers", &peers); err != nil {
		return nil, err
	}

	return &peers, nil
}

func (c *Client) StreamProgress(ctx context.Context, fileHash string) (<-chan *protocol.WSProgressMessage, <-chan error, error) {
	wsURL, err := c.getWebSocketURL()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get WebSocket URL: %w", err)
	}

	if fileHash != "" {
		wsURL += "?file_hash=" + url.QueryEscape(fileHash)
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	progressChan := make(chan *protocol.WSProgressMessage, 10)
	errorChan := make(chan error, 1)

	go func() {
		defer close(progressChan)
		defer close(errorChan)
		defer conn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				var msg map[string]interface{}
				if err := conn.ReadJSON(&msg); err != nil {
					if ctx.Err() == nil {
						errorChan <- fmt.Errorf("failed to read message: %w", err)
					}
					return
				}

				msgType, ok := msg["type"].(string)
				if !ok {
					continue
				}

				switch msgType {
				case "progress":
					var progress protocol.WSProgressMessage
					data, _ := json.Marshal(msg)
					if err := json.Unmarshal(data, &progress); err == nil {
						select {
						case progressChan <- &progress:
						case <-ctx.Done():
							return
						}
					}

				case "error":
					var errMsg protocol.WSErrorMessage
					data, _ := json.Marshal(msg)
					if err := json.Unmarshal(data, &errMsg); err == nil {
						errorChan <- fmt.Errorf("%s: %s", errMsg.Error, errMsg.Message)
						return
					}

				case "complete":
					return
				}
			}
		}
	}()

	return progressChan, errorChan, nil
}

func (c *Client) ResetMetrics() error {
	var result map[string]interface{}
	return c.post("/api/v1/metrics/reset", nil, &result)
}

func (c *Client) Health() error {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned status %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) post(endpoint string, body interface{}, result interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, c.baseURL+endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed (status %d): %s", resp.StatusCode, string(body))
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

func (c *Client) get(endpoint string, result interface{}) error {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed (status %d): %s", resp.StatusCode, string(body))
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

func (c *Client) getWebSocketURL() (string, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse base URL: %w", err)
	}

	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}

	u.Path = "/ws"

	return u.String(), nil
}

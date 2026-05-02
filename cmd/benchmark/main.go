package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	"P2P-CDN/pkg/bandwidth"
	"P2P-CDN/pkg/metrics"
)

var (
	flagURL     string
	flagOutput  string
	flagRuns    int
	flagTimeout int
	flagSave    string
	flagLimit   string
)

var rootCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "HTTP/HTTPS benchmark — coleta as mesmas métricas do P2P CDN",
	Long: `Faz download de um arquivo via HTTP/HTTPS e registra as mesmas métricas
que o daemon P2P CDN (throughput, TTFB, connection time, etc.) no mesmo
formato metrics.json, permitindo comparação direta entre as arquiteturas.`,
	RunE: run,
}

func init() {
	rootCmd.Flags().StringVarP(&flagURL, "url", "u", "", "URL para download (obrigatório)")
	rootCmd.Flags().StringVarP(&flagOutput, "output", "o", "", "Salvar arquivo baixado neste caminho (opcional)")
	rootCmd.Flags().IntVarP(&flagRuns, "runs", "n", 1, "Número de execuções")
	rootCmd.Flags().IntVar(&flagTimeout, "timeout", 30, "Timeout da requisição HTTP em segundos")
	rootCmd.Flags().StringVarP(&flagSave, "save", "s", "benchmark_metrics.json", "Arquivo de métricas para salvar os resultados")
	rootCmd.Flags().StringVarP(&flagLimit, "limit", "l", "unlimited", "Limite de download (ex: 10mbps, 50mb, 100mbit, unlimited)")
	_ = rootCmd.MarkFlagRequired("url")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(_ *cobra.Command, _ []string) error {
	limitBps, err := bandwidth.Parse(flagLimit)
	if err != nil {
		return fmt.Errorf("--limit inválido: %w", err)
	}

	mm, err := metrics.NewMetricsManager(flagSave)
	if err != nil {
		return fmt.Errorf("failed to open metrics file %s: %w", flagSave, err)
	}

	if flagRuns > 1 {
		fmt.Printf("Executando %d runs | limit: %s\n\n", flagRuns, bandwidth.Format(limitBps))
	}

	for i := 0; i < flagRuns; i++ {
		if flagRuns > 1 {
			fmt.Printf("Run %d/%d\n", i+1, flagRuns)
		}

		entry, runErr := benchmark(mm.GetNextRequestID(), limitBps)
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "run %d failed: %v\n", i+1, runErr)
		}

		if saveErr := mm.AddMetrics(entry); saveErr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not save metrics: %v\n", saveErr)
		}

		printSummary(entry)
	}

	return nil
}

func benchmark(requestID uint64, limitBps int64) (*metrics.MetricsEntry, error) {
	timeout := time.Duration(flagTimeout) * time.Second

	var (
		startTime     = time.Now()
		connectStart  time.Time
		connectDone   time.Time
		tlsStart      time.Time
		tlsDone       time.Time
		firstByteTime time.Time
		gotFirstByte  bool
	)

	trace := &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) {
			connectStart = time.Now()
		},
		ConnectDone: func(_, _ string, _ error) {
			connectDone = time.Now()
		},
		TLSHandshakeStart: func() {
			tlsStart = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			tlsDone = time.Now()
		},
		GotFirstResponseByte: func() {
			if !gotFirstByte {
				firstByteTime = time.Now()
				gotFirstByte = true
			}
		},
	}

	client := &http.Client{Timeout: timeout}

	req, err := http.NewRequest(http.MethodGet, flagURL, nil)
	if err != nil {
		return failEntry(requestID, startTime, err), err
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	if err != nil {
		return failEntry(requestID, startTime, err), err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("HTTP %d %s", resp.StatusCode, resp.Status)
		return failEntry(requestID, startTime, err), err
	}

	var src io.Reader = resp.Body
	if limitBps > 0 {
		src = newRateLimitedReader(req.Context(), resp.Body, limitBps)
	}

	hasher := sha256.New()
	var dst io.Writer = hasher

	var outFile *os.File
	if flagOutput != "" {
		outFile, err = os.Create(flagOutput)
		if err != nil {
			return failEntry(requestID, startTime, err), err
		}
		defer outFile.Close()
		dst = io.MultiWriter(hasher, outFile)
	}

	bytesReceived, copyErr := io.Copy(dst, src)

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fileHash := hex.EncodeToString(hasher.Sum(nil))

	fileSize := uint64(bytesReceived)
	if resp.ContentLength > 0 {
		fileSize = uint64(resp.ContentLength)
	}

	throughputMbps := 0.0
	if duration.Seconds() > 0 {
		throughputMbps = float64(bytesReceived*8) / (duration.Seconds() * 1_000_000)
	}

	ttfb := 0.0
	if gotFirstByte {
		ttfb = firstByteTime.Sub(startTime).Seconds()
	}

	connMs := 0.0
	if !connectStart.IsZero() && !connectDone.IsZero() {
		connMs += float64(connectDone.Sub(connectStart).Milliseconds())
	}
	if !tlsStart.IsZero() && !tlsDone.IsZero() {
		connMs += float64(tlsDone.Sub(tlsStart).Milliseconds())
	}

	errMsg := ""
	success := true
	errCount := 0
	if copyErr != nil {
		errMsg = copyErr.Error()
		success = false
		errCount = 1
	}

	return &metrics.MetricsEntry{
		RequestID:          requestID,
		FileHash:           fileHash,
		FileSize:           fileSize,
		Timestamp:          startTime,
		Duration:           duration.Seconds(),
		ThroughputMbps:     throughputMbps,
		LatencyMs:          connMs,
		TimeToFirstByteSec: ttfb,
		BytesReceived:      uint64(bytesReceived),
		PacketsReceived:    1,
		PacketLoss:         0,
		DeliveryRate:       100.0,
		CacheHit:           "no",
		Protocol:           strings.ToLower(req.URL.Scheme),
		PeersConnected:     0,
		ChunkCount:         1,
		ChunksFromP2P:      0,
		ChunksFromHTTP:     1,
		AvgChunkTimeMs:     duration.Seconds() * 1000,
		ErrorCount:         errCount,
		ErrorMessage:       errMsg,
		Success:            success,
		RetryCount:         0,
		ConnectionTimeMs:   connMs,
	}, copyErr
}

type rateLimitedReader struct {
	ctx     context.Context
	r       io.Reader
	limiter *rate.Limiter
}

func newRateLimitedReader(ctx context.Context, r io.Reader, bps int64) *rateLimitedReader {
	burst := 64 * 1024
	if int(bps) < burst {
		burst = int(bps)
	}
	return &rateLimitedReader{
		ctx:     ctx,
		r:       r,
		limiter: rate.NewLimiter(rate.Limit(bps), burst),
	}
}

func (rl *rateLimitedReader) Read(p []byte) (int, error) {
	n, err := rl.r.Read(p)
	if n > 0 {
		if waitErr := rl.limiter.WaitN(rl.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}

func failEntry(requestID uint64, startTime time.Time, err error) *metrics.MetricsEntry {
	return &metrics.MetricsEntry{
		RequestID:    requestID,
		Timestamp:    startTime,
		Duration:     time.Since(startTime).Seconds(),
		Protocol:     "https",
		CacheHit:     "no",
		DeliveryRate: 0,
		ErrorCount:   1,
		ErrorMessage: err.Error(),
		Success:      false,
	}
}

func printSummary(e *metrics.MetricsEntry) {
	status := "OK"
	if !e.Success {
		status = "FAILED"
	}
	fmt.Printf("  status:         %s\n", status)
	fmt.Printf("  protocol:       %s\n", e.Protocol)
	fmt.Printf("  file_hash:      %s\n", e.FileHash)
	fmt.Printf("  file_size:      %s\n", formatBytes(e.FileSize))
	fmt.Printf("  duration:       %.3f s\n", e.Duration)
	fmt.Printf("  throughput:     %.2f Mbps\n", e.ThroughputMbps)
	fmt.Printf("  ttfb:           %.3f s\n", e.TimeToFirstByteSec)
	fmt.Printf("  connection_ms:  %.1f ms\n", e.ConnectionTimeMs)
	if e.ErrorMessage != "" {
		fmt.Printf("  error:          %s\n", e.ErrorMessage)
	}
	fmt.Println()
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

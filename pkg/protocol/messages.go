package protocol

type RegisterFileRequest struct {
	Hash        string      `json:"hash"`
	Size        uint64      `json:"size"`
	ChunkSize   uint32      `json:"chunk_size"`
	ChunkCount  uint32      `json:"chunk_count"`
	Chunks      []ChunkInfo `json:"chunks,omitempty"`
	FallbackURL string      `json:"fallback_url"`
}

type RegisterFileResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type AnnouncePeerRequest struct {
	PeerID     string   `json:"peer_id"`
	Addrs      []string `json:"addrs"`
	FileHashes []string `json:"file_hashes"`
	NATType    string   `json:"nat_type"`
}

type AnnouncePeerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type GetPeersRequest struct {
	FileHash string `json:"file_hash"`
}

type GetPeersResponse struct {
	Peers       []PeerResponse `json:"peers"`
	FallbackURL string         `json:"fallback_url,omitempty"`
	Size        uint64         `json:"size,omitempty"`
	ChunkSize   uint32         `json:"chunk_size,omitempty"`
	ChunkCount  uint32         `json:"chunk_count,omitempty"`
	Chunks      []ChunkInfo    `json:"chunks,omitempty"`
}

type PeerResponse struct {
	PeerID   string   `json:"peer_id"`
	Addrs    []string `json:"addrs"`
	NATType  string   `json:"nat_type"`
	LastSeen string   `json:"last_seen"`
}

type UpdateStatusRequest struct {
	PeerID     string   `json:"peer_id"`
	FileHashes []string `json:"file_hashes"`
}

type UpdateStatusResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type GetNATInfoResponse struct {
	RelayServers []RelayServer `json:"relay_servers"`
	STUNServers  []string      `json:"stun_servers,omitempty"`
}

type NATInfoResponse = GetNATInfoResponse

type RelayServer struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code,omitempty"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Version   string `json:"version,omitempty"`
}

type DaemonRegisterRequest struct {
	URL        string `json:"url"`
	TrackerURL string `json:"tracker_url,omitempty"`
	AutoSeed   bool   `json:"auto_seed"`
}

type DaemonRegisterResponse struct {
	Success  bool   `json:"success"`
	FileHash string `json:"file_hash"`
	Message  string `json:"message,omitempty"`
}

type DaemonDownloadRequest struct {
	FileHash   string `json:"file_hash"`
	Output     string `json:"output"`
	TrackerURL string `json:"tracker_url,omitempty"`
}

type DaemonDownloadResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type DaemonStatusResponse struct {
	FileHash       string   `json:"file_hash"`
	Status         string   `json:"status"`
	TotalSize      uint64   `json:"total_size"`
	Downloaded     uint64   `json:"downloaded"`
	ChunksComplete uint32   `json:"chunks_complete"`
	ChunksTotal    uint32   `json:"chunks_total"`
	Peers          []string `json:"peers"`
	DownloadRate   float64  `json:"download_rate"`
	UsingFallback  bool     `json:"using_fallback"`
	FallbackURL    string   `json:"fallback_url,omitempty"`
	Error          string   `json:"error,omitempty"`
}

type DaemonPeersResponse struct {
	Peers []DaemonPeerInfo `json:"peers"`
	Count int              `json:"count"`
}

type DaemonPeerInfo struct {
	PeerID    string   `json:"peer_id"`
	Addrs     []string `json:"addrs"`
	Connected bool     `json:"connected"`
	NATType   string   `json:"nat_type,omitempty"`
}

type DaemonStopRequest struct {
	FileHash string `json:"file_hash"`
}

type DaemonStopResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type WSProgressMessage struct {
	Type           string   `json:"type"`
	FileHash       string   `json:"file_hash"`
	TotalSize      uint64   `json:"total_size"`
	Downloaded     uint64   `json:"downloaded"`
	ChunksComplete uint32   `json:"chunks_complete"`
	ChunksTotal    uint32   `json:"chunks_total"`
	Peers          []string `json:"peers"`
	DownloadRate   float64  `json:"download_rate"`
	Status         string   `json:"status"`
	UsingFallback  bool     `json:"using_fallback"`
	Timestamp      string   `json:"timestamp"`
}

type WSErrorMessage struct {
	Type      string `json:"type"`
	FileHash  string `json:"file_hash"`
	Error     string `json:"error"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type WSCompleteMessage struct {
	Type      string `json:"type"`
	FileHash  string `json:"file_hash"`
	Path      string `json:"path"`
	Size      uint64 `json:"size"`
	Timestamp string `json:"timestamp"`
}

type DaemonSeedRequest struct {
	FilePath    string `json:"file_path"`
	FallbackURL string `json:"fallback_url,omitempty"`
	TrackerURL  string `json:"tracker_url,omitempty"`
}

type DaemonSeedResponse struct {
	Success  bool   `json:"success"`
	FileHash string `json:"file_hash"`
	Message  string `json:"message,omitempty"`
}

type DaemonUnseedRequest struct {
	FileHash string `json:"file_hash"`
}

type DaemonUnseedResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type DaemonInfoResponse struct {
	PeerID          string `json:"peer_id"`
	Version         string `json:"version"`
	Uptime          string `json:"uptime,omitempty"`
	ActiveDownloads int    `json:"active_downloads"`
	ActiveSeeds     int    `json:"active_seeds"`
	ConnectedPeers  int    `json:"connected_peers"`
}

type DaemonListSeedsResponse struct {
	Seeds []DaemonSeedInfo `json:"seeds"`
	Count int              `json:"count"`
}

type DaemonSeedInfo struct {
	FileHash    string `json:"file_hash"`
	FilePath    string `json:"file_path"`
	FallbackURL string `json:"fallback_url,omitempty"`
	Size        uint64 `json:"size"`
	ChunkCount  uint32 `json:"chunk_count"`
	BytesServed uint64 `json:"bytes_served"`
	PeersServed int    `json:"peers_served"`
	StartedAt   string `json:"started_at"`
}

type DaemonListDownloadsResponse struct {
	Downloads []DaemonDownloadInfo `json:"downloads"`
	Count     int                  `json:"count"`
}

type DaemonDownloadInfo struct {
	FileHash       string  `json:"file_hash"`
	OutputPath     string  `json:"output_path"`
	Status         string  `json:"status"`
	TotalSize      uint64  `json:"total_size"`
	Downloaded     uint64  `json:"downloaded"`
	ChunksComplete uint32  `json:"chunks_complete"`
	ChunksTotal    uint32  `json:"chunks_total"`
	DownloadRate   float64 `json:"download_rate"`
	UsingFallback  bool    `json:"using_fallback"`
	StartedAt      string  `json:"started_at"`
}

type TrackerControlMessage struct {
	Type  string `json:"type"`
	Value int64  `json:"value,omitempty"`
}

type DaemonHelloMessage struct {
	Type    string `json:"type"`
	PeerID  string `json:"peer_id"`
	Version string `json:"version"`
}

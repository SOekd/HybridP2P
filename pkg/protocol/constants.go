package protocol

import "time"

const (
	Version = "0.1.0"

	DefaultChunkSize = 512 * 1024

	MinChunkSize = 256 * 1024

	MaxChunkSize = 1024 * 1024
)

const (
	BitSwapProtocol = "/ipfs/bitswap/1.2.0"
)

const (
	DefaultListenPort = 4001

	DefaultTrackerPort = 8080

	DefaultAPIPort = 8081

	DefaultTrackerURL = "http://localhost:8080"

	MaxConnections = 400

	LowWaterConnections = 100
)

var DefaultBootstrapPeers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
}

const (
	PeerAnnounceInterval = 2 * time.Minute
)

const (
	DefaultDataDir = "~/.p2pcdn/data"

	DefaultLogFile = "~/.p2pcdn/client.log"

	BlockstoreType = "flatfs"
)

const (
	TrackerRateLimit = 60
)

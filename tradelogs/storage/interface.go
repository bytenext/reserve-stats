package storage

import (
	ethereum "github.com/ethereum/go-ethereum/common"
	"time"

	"github.com/KyberNetwork/reserve-stats/lib/core"
	"github.com/KyberNetwork/reserve-stats/tradelogs/common"
)

// Interface represent a storage for TradeLogs data
type Interface interface {
	LastBlock() (int64, error)
	SaveTradeLogs(logs []common.TradeLog) error
	LoadTradeLogs(from, to time.Time) ([]common.TradeLog, error)
	GetAggregatedBurnFee(from, to time.Time, freq string, reserveAddrs []ethereum.Address) (map[ethereum.Address]map[string]float64, error)
	GetAssetVolume(token core.Token, fromTime, toTime uint64, frequency string) (map[uint64]*common.VolumeStats, error)
}

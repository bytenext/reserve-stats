package storage

import (
	"github.com/KyberNetwork/reserve-stats/lib/blockchain"
	"github.com/KyberNetwork/reserve-stats/lib/timeutil"

	"encoding/json"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

const (
	readDBName = "trade_logs"
)

func TestGetWETHETH(t *testing.T) {
	t.Skip("Skip get  WETH-ETH  on   production")
	is, err := newTestInfluxStorage(readDBName)
	if err != nil {
		t.Fatal(err)
	}
	tradeLogs, err := is.LoadTradeLogs(timeutil.TimestampMsToTime(0), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var (
		logWithBurnFeeCount = 0
		logWithETHWETH      = 0
	)
	for _, log := range tradeLogs {
		if (log.SrcAddress == blockchain.WETHAddr && log.DestAddress == blockchain.ETHAddr) || (log.SrcAddress == blockchain.ETHAddr && log.DestAddress == blockchain.WETHAddr) {
			if len(log.BurnFees) != 0 {
				logWithBurnFeeCount++
			}
			logWithETHWETH++
		}
	}
	log.Printf("Total log %d, WETH-ETH vice versal %d, WETH-ETH with burnfee %d", len(tradeLogs), logWithETHWETH, logWithBurnFeeCount)
	data, err := json.Marshal(tradeLogs)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile("weteth.json", data, 0777)
	if err != nil {
		t.Fatal(err)
	}
	t.Fail()
}

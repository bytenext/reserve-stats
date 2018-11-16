package storage

import (
	"fmt"
	"github.com/KyberNetwork/reserve-stats/lib/core"
	"github.com/KyberNetwork/reserve-stats/lib/influxdb"
	"strconv"
	"time"

	ethereum "github.com/ethereum/go-ethereum/common"
	"github.com/influxdata/influxdb/client/v2"
	"go.uber.org/zap"

	"github.com/KyberNetwork/reserve-stats/lib/blockchain"
	"github.com/KyberNetwork/reserve-stats/tradelogs/common"
	burnschema "github.com/KyberNetwork/reserve-stats/tradelogs/storage/schema/burnfee"
	logschema "github.com/KyberNetwork/reserve-stats/tradelogs/storage/schema/tradelog"
	walletschema "github.com/KyberNetwork/reserve-stats/tradelogs/storage/schema/walletfee"
)

const (
	//timePrecision is the precision configured for influxDB
	timePrecision = "ms"
)

// InfluxStorage represent a client to store trade data to influx DB
type InfluxStorage struct {
	dbName       string
	influxClient client.Client
	coreClient   core.Interface
	sugar        *zap.SugaredLogger
}

// NewInfluxStorage init an instance of InfluxStorage
func NewInfluxStorage(sugar *zap.SugaredLogger, dbName string, influxClient client.Client, coreClient core.Interface) (*InfluxStorage, error) {
	storage := &InfluxStorage{
		dbName:       dbName,
		influxClient: influxClient,
		coreClient:   coreClient,
		sugar:        sugar,
	}
	if err := storage.createDB(); err != nil {
		return nil, err
	}
	return storage, nil
}

// SaveTradeLogs persist trade logs to DB
func (is *InfluxStorage) SaveTradeLogs(logs []common.TradeLog) error {
	defer is.influxClient.Close()
	// this map will keep track on the current batch to ensure there is no duplication
	var cacheTraded = make(map[ethereum.Address]bool)
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  is.dbName,
		Precision: timePrecision,
	})
	if err != nil {
		return err
	}
	for _, log := range logs {
		points, err := is.tradeLogToPoint(log, cacheTraded)
		if err != nil {
			return err
		}

		for _, pt := range points {
			bp.AddPoint(pt)
		}
	}

	if err := is.influxClient.Write(bp); err != nil {
		return err
	}

	if len(logs) > 0 {
		is.sugar.Debugw("saved trade logs into influxdb",
			"first_block", logs[0].BlockNumber,
			"last_block", logs[len(logs)-1].BlockNumber,
			"trade_logs", len(logs))
	} else {
		is.sugar.Debugw("no trade log to store")
	}

	return is.influxClient.Close()
}

// LastBlock returns last stored trade log block number from database.
func (is InfluxStorage) LastBlock() (int64, error) {
	q := fmt.Sprintf(`SELECT "block_number","eth_amount" from "trades" ORDER BY time DESC limit 1`)

	res, err := is.queryDB(is.influxClient, q)
	if err != nil {
		return 0, err
	}

	if len(res) != 1 || len(res[0].Series) != 1 || len(res[0].Series[0].Values[0]) != 3 {
		is.sugar.Info("no result returned for last block query")
		return 0, nil
	}

	return influxdb.GetInt64FromTagValue(res[0].Series[0].Values[0][1])
}

func prepareTradeLogQuery() string {
	var (
		tradeLogQueryFields = []logschema.FieldName{
			logschema.Time,
			logschema.BlockNumber,
			logschema.EthReceivalSender,
			logschema.EthReceivalAmount,
			logschema.UserAddr,
			logschema.SrcAddr,
			logschema.DstAddr,
			logschema.SrcAmount,
			logschema.DstAmount,
			logschema.IP,
			logschema.Country,
		}
		tradeLogQuery string
	)
	for _, field := range tradeLogQueryFields {
		tradeLogQuery += field.String() + ", "
	}
	fiatAmount := fmt.Sprintf("(%s + %s) AS %s", logschema.EthAmount.String(), logschema.EthUSDRate.String(), logschema.FiatAmount.String())
	tradeLogQuery += fiatAmount
	return tradeLogQuery
}

func prepareBurnfeeQuery() string {
	var (
		burnFeeFields = []burnschema.FieldName{
			burnschema.Time,
			burnschema.ReserveAddr,
			burnschema.Amount,
			burnschema.LogIndex,
		}
		burnFeeQuery string
	)
	for i, field := range burnFeeFields {
		if i != 0 {
			burnFeeQuery += ", "
		}
		burnFeeQuery += field.String()
	}
	return burnFeeQuery
}

func prepareWalletFeeQuery() string {
	var (
		walletFeeFields = []walletschema.FieldName{
			walletschema.Time,
			walletschema.ReserveAddr,
			walletschema.WalletAddr,
			walletschema.Amount,
			walletschema.LogIndex,
		}
		walletQuery string
	)
	for i, field := range walletFeeFields {
		if i != 0 {
			walletQuery += ", "
		}
		walletQuery += field.String()
	}
	return walletQuery
}

// LoadTradeLogs return trade logs from DB
func (is *InfluxStorage) LoadTradeLogs(from, to time.Time) ([]common.TradeLog, error) {
	var (
		result = make([]common.TradeLog, 0)
		q      = fmt.Sprintf(
			`
		SELECT %[1]s FROM %[6]s WHERE time >= '%[4]s' AND time <= '%[5]s' GROUP BY %[9]s;
		SELECT %[2]s FROM %[7]s WHERE time >= '%[4]s' AND time <= '%[5]s' GROUP BY %[10]s;
		SELECT %[3]s FROM %[8]s WHERE time >= '%[4]s' AND time <= '%[5]s' GROUP BY %[11]s;
		`,
			prepareBurnfeeQuery(),
			prepareWalletFeeQuery(),
			prepareTradeLogQuery(),
			from.Format(time.RFC3339),
			to.Format(time.RFC3339),
			common.BurnfeeMeasurementName,
			common.WalletMeasurementName,
			common.TradeLogMeasurementName,
			burnschema.TxHash.String()+", "+burnschema.TradeLogIndex.String(),
			walletschema.TxHash.String()+", "+walletschema.TradeLogIndex.String(),
			logschema.TxHash.String()+", "+logschema.LogIndex.String(),
		)

		logger = is.sugar.With(
			"func", "tradelogs/storage/InfluxStorage.LoadTradLogs",
			"from", from,
			"to", to,
		)
	)
	logger.Debug("prepared query statement", "query", q)

	res, err := is.queryDB(is.influxClient, q)
	if err != nil {
		return nil, err
	}

	// Get BurnFees
	if len(res[0].Series) == 0 {
		is.sugar.Debug("empty burn fee in query result")
		return nil, nil
	}

	// map [tx_hash][trade_log_index][]common.BurnFee
	burnFeesByTxHash := make(map[ethereum.Hash]map[uint][]common.BurnFee)
	for _, row := range res[0].Series {
		txHash, tradeLogIndex, burnFees, err := is.rowToBurnFees(row)
		if err != nil {
			return nil, err
		}
		_, exist := burnFeesByTxHash[txHash]
		if !exist {
			burnFeesByTxHash[txHash] = make(map[uint][]common.BurnFee)
		}
		burnFeesByTxHash[txHash][uint(tradeLogIndex)] = burnFees
	}

	// Get WalletFees
	// map [tx_hash][trade_log_index][]common.WalletFee
	walletFeesByTxHash := make(map[ethereum.Hash]map[uint][]common.WalletFee)

	if len(res[1].Series) == 0 {
		is.sugar.Debug("empty wallet fee in query result")
	} else {
		for _, row := range res[1].Series {
			txHash, tradeLogIndex, walletFees, err := is.rowToWalletFees(row)
			if err != nil {
				return nil, err
			}
			_, exist := walletFeesByTxHash[txHash]
			if !exist {
				walletFeesByTxHash[txHash] = make(map[uint][]common.WalletFee)
			}
			walletFeesByTxHash[txHash][uint(tradeLogIndex)] = walletFees
		}
	}

	// Get TradeLogs
	if len(res[2].Series) == 0 {
		is.sugar.Debug("empty trades in query result")
		return result, nil
	}

	for _, row := range res[2].Series {
		tradeLog, err := is.rowToTradeLog(row, burnFeesByTxHash, walletFeesByTxHash)
		if err != nil {
			return nil, err
		}
		result = append(result, tradeLog)
	}

	return result, nil
}

// createDB creates the database will be used for storing trade logs measurements.
func (is *InfluxStorage) createDB() error {
	_, err := is.queryDB(is.influxClient, fmt.Sprintf("CREATE DATABASE %s", is.dbName))
	return err
}

// queryDB convenience function to query the database
func (is *InfluxStorage) queryDB(clnt client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: is.dbName,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func (is *InfluxStorage) tradeLogToPoint(log common.TradeLog, cacheTradeds map[ethereum.Address]bool) ([]*client.Point, error) {
	var points []*client.Point

	tags := map[string]string{
		logschema.BlockNumber.String(): strconv.FormatUint(log.BlockNumber, 10),
		logschema.TxHash.String():      log.TransactionHash.String(),

		logschema.EthReceivalSender.String(): log.EtherReceivalSender.String(),

		logschema.UserAddr.String(): log.UserAddress.String(),

		logschema.SrcAddr.String(): log.SrcAddress.String(),
		logschema.DstAddr.String(): log.DestAddress.String(),

		logschema.Country.String(): log.Country,
		logschema.IP.String():      log.IP,

		logschema.EthUSDProvider.String(): log.ETHUSDProvider,
		logschema.LogIndex.String():       strconv.FormatUint(uint64(log.Index), 10),
	}

	logger := is.sugar.With(
		"func", "tradelogs/storage/tradeLogToPoint",
		"log", log,
	)

	if blockchain.IsBurnable(log.SrcAddress) {
		if blockchain.IsBurnable(log.DestAddress) {
			if len(log.BurnFees) == 2 {
				tags[logschema.SrcReserveAddr.String()] = log.BurnFees[0].ReserveAddress.String()
				tags[logschema.DstReserveAddr.String()] = log.BurnFees[1].ReserveAddress.String()
			} else {
				logger.Warnw("unexpected burn fees", "got", log.BurnFees, "want", "2 burn fees (src-dst)")
			}
		} else {
			if len(log.BurnFees) == 1 {
				tags[logschema.SrcReserveAddr.String()] = log.BurnFees[0].ReserveAddress.String()
			} else {
				logger.Warnw("unexpected burn fees", "got", log.BurnFees, "want", "1 burn fees (src)")
			}
		}
	} else if blockchain.IsBurnable(log.DestAddress) {
		if len(log.BurnFees) == 1 {
			tags[logschema.DstReserveAddr.String()] = log.BurnFees[0].ReserveAddress.String()
		} else {
			logger.Warnw("unexpected burn fees", "got", log.BurnFees, "want", "1 burn fees (dst)")
		}
	}

	ethReceivalAmount, err := is.coreClient.FromWei(blockchain.ETHAddr, log.EtherReceivalAmount)
	if err != nil {
		return nil, err
	}

	srcAmount, err := is.coreClient.FromWei(log.SrcAddress, log.SrcAmount)
	if err != nil {
		return nil, err
	}

	dstAmount, err := is.coreClient.FromWei(log.DestAddress, log.DestAmount)
	if err != nil {
		return nil, err
	}

	var ethAmount float64

	if log.SrcAddress == blockchain.ETHAddr {
		ethAmount = srcAmount
	} else if log.DestAddress == blockchain.ETHAddr {
		ethAmount = dstAmount
	} else {
		ethAmount = ethReceivalAmount
	}

	fields := map[string]interface{}{
		logschema.EthReceivalAmount.String(): ethReceivalAmount,

		logschema.SrcAmount.String():  srcAmount,
		logschema.DstAmount.String():  dstAmount,
		logschema.EthUSDRate.String(): log.ETHUSDRate,

		logschema.EthAmount.String(): ethAmount,
	}

	tradePoint, err := client.NewPoint(common.TradeLogMeasurementName, tags, fields, log.Timestamp)
	if err != nil {
		return nil, err
	}

	points = append(points, tradePoint)

	// build burnFeePoint
	for _, burn := range log.BurnFees {
		tags := map[string]string{
			burnschema.TxHash.String():        log.TransactionHash.String(),
			burnschema.ReserveAddr.String():   burn.ReserveAddress.String(),
			burnschema.LogIndex.String():      strconv.FormatUint(uint64(burn.Index), 10),
			burnschema.TradeLogIndex.String(): strconv.FormatUint(uint64(log.Index), 10),
		}

		burnAmount, err := is.coreClient.FromWei(blockchain.KNCAddr, burn.Amount)
		if err != nil {
			return nil, err
		}

		fields := map[string]interface{}{
			burnschema.Amount.String(): burnAmount,
		}

		burnPoint, err := client.NewPoint(common.BurnfeeMeasurementName, tags, fields, log.Timestamp)
		if err != nil {
			return nil, err
		}

		points = append(points, burnPoint)
	}

	// build walletFeePoint
	for _, walletFee := range log.WalletFees {
		tags := map[string]string{
			walletschema.TxHash.String():        log.TransactionHash.String(),
			walletschema.ReserveAddr.String():   walletFee.ReserveAddress.String(),
			walletschema.WalletAddr.String():    walletFee.WalletAddress.String(),
			walletschema.LogIndex.String():      strconv.FormatUint(uint64(walletFee.Index), 10),
			walletschema.TradeLogIndex.String(): strconv.FormatUint(uint64(log.Index), 10),
		}

		amount, err := is.coreClient.FromWei(blockchain.KNCAddr, walletFee.Amount)
		if err != nil {
			return nil, err
		}

		fields := map[string]interface{}{
			walletschema.Amount.String(): amount,
		}

		walletFeePoint, err := client.NewPoint(common.WalletMeasurementName, tags, fields, log.Timestamp)
		if err != nil {
			return nil, err
		}

		points = append(points, walletFeePoint)
	}

	_, cacheTraded := cacheTradeds[log.UserAddress]
	if !cacheTraded {
		traded, err := is.userTraded(log.UserAddress)
		if err != nil {
			return nil, err
		}
		if !traded {
			cacheTradeds[log.UserAddress] = true
			logger.Debugw("user first trade", "user_addr", log.UserAddress.String())
			tags := map[string]string{
				"user_addr": log.UserAddress.String(),
			}
			fields := map[string]interface{}{
				"traded": true,
			}
			firstTradePt, err := client.NewPoint("first_trades", tags, fields, log.Timestamp)
			if err != nil {
				return nil, err
			}
			points = append(points, firstTradePt)
		}
	}
	return points, nil
}

func (is InfluxStorage) userTraded(addr ethereum.Address) (bool, error) {
	q := fmt.Sprintf("SELECT traded FROM first_trades WHERE user_addr='%s'", addr.String())
	response, err := is.queryDB(is.influxClient, q)
	if err != nil {
		return false, err
	}
	// if there is no record, this mean the address has not traded yet
	if (len(response) == 0) || (len(response[0].Series) == 0) || (len(response[0].Series[0].Values) == 0) {
		return false, nil
	}
	return true, nil
}

package crawler

import (
	"fmt"
	"golang.org/x/sync/errgroup"
	"sync"

	"github.com/KyberNetwork/reserve-stats/lib/contracts"
	"github.com/KyberNetwork/reserve-stats/lib/core"
	rsvRateCommon "github.com/KyberNetwork/reserve-stats/reserverates/common"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethereum "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

// ReserveRatesCrawler contains two wrapper contracts for V1 and V2 contract,
// a set of addresses to crawl rates from and setting object to query for reserve's token settings
type ReserveRatesCrawler struct {
	sugar           *zap.SugaredLogger
	wrapperContract reserveRateGetter
	addresses       []ethereum.Address
	stg             supportedTokensGetter
}

// NewReserveRatesCrawler returns an instant of ReserveRatesCrawler.
func NewReserveRatesCrawler(sugar *zap.SugaredLogger, addrs []string, client *ethclient.Client) (*ReserveRatesCrawler, error) {
	wrpContract, err := contracts.NewVersionedWrapperFallback(sugar, client)
	if err != nil {
		return nil, err
	}

	var ethAddrs []ethereum.Address
	for _, addr := range addrs {
		ethAddrs = append(ethAddrs, ethereum.HexToAddress(addr))
	}
	return &ReserveRatesCrawler{
		sugar:           sugar,
		wrapperContract: wrpContract,
		addresses:       ethAddrs,
		stg:             newCoreSupportedTokens(sugar, client),
	}, nil
}

func (rrc *ReserveRatesCrawler) getEachReserveRate(block uint64, rsvAddr ethereum.Address) (map[string]rsvRateCommon.ReserveRateEntry, error) {
	var (
		err           error
		rates         = make(map[string]rsvRateCommon.ReserveRateEntry)
		srcAddresses  []ethereum.Address
		destAddresses []ethereum.Address
	)

	logger := rrc.sugar.With(
		"func", "reserverates/crawler/ReserveRatesCrawler.getEachReserveRate",
		"block", block,
		"reserve_address", rsvAddr.Hex(),
	)
	logger.Debug("fetching reserve rates")

	tokens, err := rrc.stg.supportedTokens(rsvAddr, block)
	if err != nil {
		if err.Error() == bind.ErrNoCode.Error() {
			logger.Infow("reserve contract does not exist")
			return nil, nil
		}
		return nil, fmt.Errorf("cannot get supported tokens for reserve %s. Error: %s", rsvAddr.Hex(), err)
	}

	for _, token := range tokens {
		srcAddresses = append(srcAddresses, token, ethereum.HexToAddress(core.ETHToken.Address))
		destAddresses = append(destAddresses, ethereum.HexToAddress(core.ETHToken.Address), token)
	}

	reserveRates, sanityRates, err := rrc.wrapperContract.GetReserveRate(block, rsvAddr, srcAddresses, destAddresses)
	if err != nil {
		return nil, fmt.Errorf("cannot get rates for reserve %s. Error: %s", rsvAddr.Hex(), err)
	}

	for index, token := range tokens {
		symbol, err := rrc.stg.symbol(token)
		if err != nil {
			return nil, err
		}
		rates[fmt.Sprintf("ETH-%s", symbol)] = rsvRateCommon.NewReserveRateEntry(reserveRates, sanityRates, index)
	}

	logger.Debug("reserve rates fetched successfully")
	return rates, err
}

// GetReserveRates returns the map[ReserveAddress]ReserveRates at the given block number.
// It will only return rates from the set of addresses within its definition.
func (rrc *ReserveRatesCrawler) GetReserveRates(block uint64) (map[string]map[string]rsvRateCommon.ReserveRateEntry, error) {
	var (
		err    error
		g      errgroup.Group
		data   = sync.Map{}
		result = make(map[string]map[string]rsvRateCommon.ReserveRateEntry)
	)

	logger := rrc.sugar.With(
		"func", "reserverates/crawler/ReserveRatesCrawler.GetReserveRates",
		"block", block,
		"reserves", len(rrc.addresses),
	)
	logger.Debug("fetching rates for all reserves")

	for _, rsvAddr := range rrc.addresses {
		// copy to local variables to avoid race condition
		block, rsvAddr := block, rsvAddr
		g.Go(func() error {
			rates, err := rrc.getEachReserveRate(block, rsvAddr)
			if err != nil {
				return err
			}

			if rates == nil {
				logger.Info("rates is not available, skipping")
				return nil
			}

			data.Store(rsvAddr, rates)
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, err
	}

	data.Range(func(key, value interface{}) bool {
		reserveAddr, ok := key.(ethereum.Address)
		//if there is conversion error, continue to next key,val
		if !ok {
			err = fmt.Errorf("key (%v) cannot be asserted to ethereum.Address", key)
			return false
		}
		rates, ok := value.(map[string]rsvRateCommon.ReserveRateEntry)
		if !ok {
			err = fmt.Errorf("value (%v) cannot be asserted to reserveRates", value)
			return false
		}
		result[reserveAddr.Hex()] = rates
		return true
	})
	if err != nil {
		return nil, err
	}

	return result, err
}

package workers

import (
	"reflect"
	"sync"
	"time"

	"github.com/KyberNetwork/reserve-stats/lib/app"
	"github.com/KyberNetwork/reserve-stats/lib/blockchain"
	"github.com/KyberNetwork/reserve-stats/lib/contracts"
	"github.com/KyberNetwork/reserve-stats/lib/core"
	"github.com/KyberNetwork/reserve-stats/reserverates/common"
	"github.com/KyberNetwork/reserve-stats/reserverates/crawler"
	"github.com/KyberNetwork/reserve-stats/reserverates/storage"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

type job interface {
	execute(sugar *zap.SugaredLogger) (map[string]common.ReserveRates, error)
	info() (order int, block uint64)
}

// FetcherJob represent a job to crawl rates at given block
type FetcherJob struct {
	c        *cli.Context
	order    int
	block    uint64
	attempts int
	addrs    []string
}

// NewFetcherJob return an instance of FetcherJob
func NewFetcherJob(c *cli.Context, order int, block uint64, addrs []string, attempts int) *FetcherJob {
	return &FetcherJob{
		c:        c,
		order:    order,
		block:    block,
		attempts: attempts,
		addrs:    addrs,
	}
}

// retry the given fn function for attempts time with sleep duration between before returns an error.
func retry(fn func(*zap.SugaredLogger) (map[string]common.ReserveRates, error), attempts int, logger *zap.SugaredLogger) (map[string]common.ReserveRates, error) {
	var (
		result map[string]common.ReserveRates
		err    error
	)

	for i := 0; i < attempts; i++ {
		if result, err = fn(logger); err == nil {
			return result, nil
		}

		logger.Debugw("failed to execute job",
			"attempt", i,
			"err", err.Error(),
		)
		time.Sleep(time.Second)
	}

	return nil, err
}

func (fj *FetcherJob) fetch(sugar *zap.SugaredLogger) (map[string]common.ReserveRates, error) {
	client, err := app.NewEthereumClientFromFlag(fj.c)
	if err != nil {
		return nil, err
	}

	blockTimeResolver, err := blockchain.NewBlockTimeResolver(sugar, client)
	if err != nil {
		return nil, err
	}

	coreClient, err := core.NewClientFromContext(sugar, fj.c)
	if err != nil {
		return nil, err
	}

	internalReserveAddress := contracts.InternalReserveAddress().MustGetOneFromContext(fj.c)

	ratesCrawler, err := crawler.NewReserveRatesCrawler(sugar, fj.addrs, client, coreClient, internalReserveAddress, blockTimeResolver)
	if err != nil {
		return nil, err
	}

	rates, err := ratesCrawler.GetReserveRates(fj.block)
	if err != nil {
		return nil, err
	}

	return rates, nil
}

func (fj *FetcherJob) execute(sugar *zap.SugaredLogger) (map[string]common.ReserveRates, error) {
	return retry(fj.fetch, fj.attempts, sugar)
}

func (fj *FetcherJob) info() (int, uint64) {
	return fj.order, fj.block
}

// Pool represents a group of workers
type Pool struct {
	sugar *zap.SugaredLogger

	wg sync.WaitGroup

	jobCh chan job
	errCh chan error

	mutex                 *sync.Mutex
	lastCompletedJobOrder int // Keep the order of the last completed job

	rateStorage storage.ReserveRatesStorage
	gr          *common.GroupedReserveRates
}

// NewPool returns a pool of workers
func NewPool(sugar *zap.SugaredLogger, maxWorkers int, rateStorage storage.ReserveRatesStorage) *Pool {
	var pool = &Pool{
		sugar:                 sugar,
		jobCh:                 make(chan job),
		errCh:                 make(chan error, maxWorkers),
		mutex:                 &sync.Mutex{},
		lastCompletedJobOrder: 0,
		rateStorage:           rateStorage,
	}

	pool.wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func(sugar *zap.SugaredLogger, workerID int) {
			logger := sugar.With("worker_id", workerID)
			logger.Infow("starting worker",
				"func", "reserverates/workers/NewPool",
				"max_workers", maxWorkers)

			for j := range pool.jobCh {
				order, block := j.info()
				rates, err := j.execute(sugar)
				if err != nil {
					logger.Errorw("fetcher job execution failed", "block", block, "err", err)
					pool.errCh <- err
					break
				}

				logger.Infow("fetcher job executed successfully", "block", block)
				if err = pool.storeRates(order, block, rates); err != nil {
					pool.errCh <- err
					break
				}

				//// try to save rate into db until success
				//for saveSuccess := false; saveSuccess == false; time.Sleep(time.Second) {
				//	var err error
				//
				//	pool.mutex.Lock()
				//	if order == pool.lastCompletedJobOrder+1 {
				//		if err = pool.rateStorage.UpdateRatesRecords(*common.GroupedReserveRates); err == nil {
				//			logger.Debugw("reserve rates is stored successfully", "order", order)
				//			saveSuccess = true
				//			pool.lastCompletedJobOrder++
				//		}
				//	}
				//	pool.mutex.Unlock()
				//
				//	if err != nil {
				//		logger.Errorw("save rates into db failed",
				//			"block", block,
				//			"err", err)
				//		pool.errCh <- err
				//		break
				//	}
				//}
				//
				//logger.Infow("save rates into db success", "block", block)
			}

			logger.Infow("worker stopped",
				"func", "reserverates/workers/NewPool",
				"max_workers", maxWorkers)
			pool.wg.Done()
		}(sugar, i)
	}

	return pool
}

// GetLastCompleteJobOrder return the order of the latest completed job
func (p *Pool) GetLastCompleteJobOrder() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.lastCompletedJobOrder
}

// Run puts new job to queue
func (p *Pool) Run(j job) {
	order, block := j.info()
	p.sugar.Infow("putting new job to queue",
		"func", "reserverates/workers/Run",
		"order", order,
		"block", block)
	p.jobCh <- j
}

// Shutdown stops the workers pool
func (p *Pool) Shutdown() {
	p.sugar.Infow("workers pool shutting down",
		"func", "reserverates/workers/Shutdown")
	close(p.jobCh)
	p.wg.Wait()
	close(p.errCh)
}

// ErrCh returns error reporting channel of workers pool.
func (p *Pool) ErrCh() chan error {
	return p.errCh
}

func (p *Pool) storeRates(order int, blockNumber uint64, rates map[string]common.ReserveRates) error {
	var logger = p.sugar.With(
		"func", "reserverates/workers/Pool.storeRates",
		"order", order,
		"block_number", blockNumber,
	)

	for {
		p.mutex.Lock()
		if order != p.lastCompletedJobOrder+1 {
			logger.Debugw("waiting for next job to complete", "last_completed", p.lastCompletedJobOrder)
			time.Sleep(time.Second / 2)
			p.mutex.Unlock()
			continue
		}

		logger.Debugw("previous job is completed", "last_completed", p.lastCompletedJobOrder)

		if p.gr == nil {
			logger.Debugw("starting new reserve rates group")
			p.gr = &common.GroupedReserveRates{
				// TODO: fill timestamp
				// Timestamp: time.Time{},
				FromBlock: blockNumber,
				ToBlock:   blockNumber + 1,
				Rates:     rates,
			}
			break
		}

		if reflect.DeepEqual(p.gr.Rates, rates) {
			logger.Debugw("rates does not change, adding to current group",
				"from_block", p.gr.FromBlock)
			p.gr.ToBlock++
			break
		}

		logger.Debugw("rates changed, storing to database",
			"from_block", p.gr.FromBlock,
			"rates", rates,
			"grouped_rates", p.gr.Rates,
		)
		p.rateStorage.UpdateRatesRecords(p.gr)
		p.gr = nil
		break
	}

	p.lastCompletedJobOrder++
	p.mutex.Unlock()
	return nil
}

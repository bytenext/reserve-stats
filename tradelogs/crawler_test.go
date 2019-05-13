package tradelogs

import (
	"math/big"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/KyberNetwork/reserve-stats/lib/blockchain"
	"github.com/KyberNetwork/reserve-stats/lib/deployment"
	"github.com/KyberNetwork/reserve-stats/lib/testutil"
	"github.com/KyberNetwork/reserve-stats/lib/tokenrate"
	"github.com/KyberNetwork/reserve-stats/tradelogs/common"
)

type mockBroadCastClient struct{}

func newMockBroadCastClient() *mockBroadCastClient {
	return &mockBroadCastClient{}
}

func (*mockBroadCastClient) GetTxInfo(tx string) (string, string, string, error) {
	return "123", "8.8.8.8", "US", nil
}

func assertTradeLog(t *testing.T, tradeLog common.TradeLog) {
	t.Helper()

	assert.NotZero(t, tradeLog.Timestamp)
	assert.NotZero(t, tradeLog.BlockNumber)
	assert.NotZero(t, tradeLog.TransactionHash)

	if tradeLog.SrcAddress != blockchain.ETHAddr {
		assert.NotZero(t, tradeLog.EthAmount)
	}

	assert.NotZero(t, tradeLog.UserAddress)
	assert.NotZero(t, tradeLog.SrcAddress)
	assert.NotZero(t, tradeLog.DestAddress)
	assert.NotZero(t, tradeLog.SrcAmount)
	assert.NotZero(t, tradeLog.DestAmount)
	assert.NotZero(t, tradeLog.ReceiverAddress)
	assert.True(t, !blockchain.IsZeroAddress(tradeLog.SrcReserveAddress) || !blockchain.IsZeroAddress(tradeLog.DstReserveAddress))

	if blockchain.IsBurnable(tradeLog.SrcAddress) || blockchain.IsBurnable(tradeLog.DestAddress) {
		assert.NotZero(t, tradeLog.BurnFees)
	}

	assert.NotZero(t, tradeLog.ETHUSDRate)
	assert.NotZero(t, tradeLog.ETHUSDProvider)
	assert.NotZero(t, tradeLog.Index)
}

func TestCrawlerGetTradeLogs(t *testing.T) {
	testutil.SkipExternal(t)

	sugar := testutil.MustNewDevelopmentSugaredLogger()
	client := testutil.MustNewDevelopmentwEthereumClient()

	// v3 contract addresses
	v3Addresses := []ethereum.Address{
		ethereum.HexToAddress("0x818E6FECD516Ecc3849DAf6845e3EC868087B755"), // network contract
		ethereum.HexToAddress("0x9ae49C0d7F8F9EF4B864e004FE86Ac8294E20950"), // internal network contract
		ethereum.HexToAddress("0x52166528FCC12681aF996e409Ee3a421a4e128A3"), // burner contract
	}
	c, err := NewCrawler(
		sugar,
		client,
		newMockBroadCastClient(),
		tokenrate.NewMock(),
		v3Addresses,
		deployment.StartingBlocks[deployment.Production])
	require.NoError(t, err)

	tradeLogs, err := c.GetTradeLogs(big.NewInt(7025000), big.NewInt(7025100), time.Minute)
	require.NoError(t, err)
	require.Len(t, tradeLogs, 10)
	for _, tradeLog := range tradeLogs {
		assertTradeLog(t, tradeLog)
	}

	// v2 contract addresses
	v2Addresses := []ethereum.Address{
		ethereum.HexToAddress("0x818E6FECD516Ecc3849DAf6845e3EC868087B755"), // network contract
		ethereum.HexToAddress("0x91a502C678605fbCe581eae053319747482276b9"), // internal network contract
		ethereum.HexToAddress("0xed4f53268bfdFF39B36E8786247bA3A02Cf34B04"), // burner contract
	}

	c, err = NewCrawler(
		sugar,
		client,
		newMockBroadCastClient(),
		tokenrate.NewMock(),
		v2Addresses,
		deployment.StartingBlocks[deployment.Production])
	require.NoError(t, err)

	tradeLogs, err = c.GetTradeLogs(big.NewInt(6343120), big.NewInt(6343220), time.Minute)
	require.NoError(t, err)
	require.Len(t, tradeLogs, 8)
	for _, tradeLog := range tradeLogs {
		assertTradeLog(t, tradeLog)
	}

	// v1 contract addresses
	v1Addresses := []ethereum.Address{
		ethereum.HexToAddress("0x818E6FECD516Ecc3849DAf6845e3EC868087B755"), // network contract
		ethereum.HexToAddress("0x964F35fAe36d75B1e72770e244F6595B68508CF5"), // internal network contract
		ethereum.HexToAddress("0x07f6e905f2a1559cd9fd43cb92f8a1062a3ca706"), // burner contract
	}

	c, err = NewCrawler(
		sugar,
		client,
		newMockBroadCastClient(),
		tokenrate.NewMock(),
		v1Addresses,
		deployment.StartingBlocks[deployment.Production])
	require.NoError(t, err)

	tradeLogs, err = c.GetTradeLogs(big.NewInt(5877442), big.NewInt(5877500), time.Minute)
	require.NoError(t, err)
	require.Len(t, tradeLogs, 8)
	for _, tradeLog := range tradeLogs {
		assertTradeLog(t, tradeLog)
	}

	// block: 6343124
	// tx: 0x4959968da434aa9b1da585479a19da0ce71c47b6e896aab85c6a285400d6de18
	// conversion: WETH --> ETH
	var sampleTxHash = "0x4959968da434aa9b1da585479a19da0ce71c47b6e896aab85c6a285400d6de18"
	var found = false
	for _, tradeLog := range tradeLogs {
		if tradeLog.TransactionHash == ethereum.HexToHash(sampleTxHash) {
			found = true
			assert.Equal(t, ethereum.HexToAddress("0x57f8160e1c59d16c01bbe181fd94db4e56b60495"), tradeLog.SrcReserveAddress,
				"WETH --> ETH trade log must have source reserve address")

			assert.Equal(t, ethereum.HexToAddress("0xd064e4c8f55cff3f45f2e5af5d24bdf0107fe40e"), tradeLog.ReceiverAddress,
				"Tradelog must have receiver address")
		}
	}
	assert.True(t, found, "transaction %s not found", sampleTxHash)

	tradeLogs, err = c.GetTradeLogs(big.NewInt(6325136), big.NewInt(6325137), time.Minute)
	require.NoError(t, err)
	require.Len(t, tradeLogs, 3)
	for _, tradeLog := range tradeLogs {
		assertTradeLog(t, tradeLog)
	}

	// block: 6325136
	// tx: 0xa1a0cec06413c5466f46d64bc8d6aa2606e82e2da466ec7b266331c056e20133
	// conversion: ETH --> WETH
	sampleTxHash = "0xa1a0cec06413c5466f46d64bc8d6aa2606e82e2da466ec7b266331c056e20133"
	found = false
	for _, tradeLog := range tradeLogs {
		if tradeLog.TransactionHash == ethereum.HexToHash(sampleTxHash) {
			found = true
			assert.Equal(t,
				ethereum.HexToAddress("0x57f8160e1c59d16c01bbe181fd94db4e56b60495"),
				tradeLog.SrcReserveAddress,
				"ETH --> WETH trade log must have dest reserve address")

			// assert.Equal(t, ethereum.HexToAddress("0xf214dde57f32f3f34492ba3148641693058d4a9e"), tradeLog.ReceiverAddress,
			// 	"Tradelog must have receiver address")
		}
	}
	assert.True(t, found, "transaction %s not found", sampleTxHash)

	// block: 7000184
	// tx: 0xbda96c208fee7812f463f1fff515a1c70d9148ffe8b40a91db419a10074d4cc1
	// conversion : ETH-GTO
	// ethAmount must equal to : 749378067533693720
	tradeLogs, err = c.GetTradeLogs(big.NewInt(7000184), big.NewInt(7000184), time.Minute)
	require.NoError(t, err)
	require.Len(t, tradeLogs, 1)
	for _, tradeLog := range tradeLogs {
		assertTradeLog(t, tradeLog)
	}
	sampleTxHash = "0xbda96c208fee7812f463f1fff515a1c70d9148ffe8b40a91db419a10074d4cc1"
	found = false
	for _, tradeLog := range tradeLogs {
		if tradeLog.TransactionHash == ethereum.HexToHash(sampleTxHash) {
			found = true
			assert.Equal(t,
				big.NewInt(749378067533693720),
				tradeLog.EthAmount,
				"trade log's ETH amount must equal to the 749378067533693720")

			// assert.Equal(t, ethereum.HexToAddress("0x85c5c26dc2af5546341fc1988b9d178148b4838b"), tradeLog.ReceiverAddress,
			// 	"Tradelog must have receiver address")
		}
	}
	assert.True(t, found, "transaction %s not found", sampleTxHash)

}

package common

import "github.com/ethereum/go-ethereum/common"

// Application represents a third party application powered by KyberNetwork.
type Application struct {
	ID        int64            `json:"id,omitempty"`
	Name      string           `json:"name" binding:"required"`
	Addresses []common.Address `json:"addresses" binding:"required"`
}

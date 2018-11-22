package userprofile

import (
	ethereum "github.com/ethereum/go-ethereum/common"
)

//MockClient is the mock implementation of user profile Interface
type MockClient struct{}

func (m MockClient) LookUpUserProfile(addr ethereum.Address) (UserProfile, error) {
	return UserProfile{
		UserName:  "mockUserName",
		ProfileID: "mockID",
	}, nil
}

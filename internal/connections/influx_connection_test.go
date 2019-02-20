package connections

import (
	"testing"
)

func TestInfluxConnectionServiceNewConnection(t *testing.T) {
	service := &defaultInfluxConnectionService{}
	if _, err := service.NewConnection(nil); err == nil {
		t.Error("should not be able to create a client without connection params")
	}

	params := &InfluxConnectionParams{}
	if _, err := service.NewConnection(params); err == nil {
		t.Error("server address should not be accepted")
	}

	params.Server = "http://someaddress"
	if res, err := service.NewConnection(params); err != nil || res == nil {
		t.Error("client should have been created without errors")
	}

	//increase coverage
	params.Username = "hyuck"
	params.Password = "hyuck"
	if res, err := service.NewConnection(params); err != nil || res == nil {
		t.Error("client should have been created")
	}
}

package connections

import "testing"

func TestInfluxConnectionServiceNewConnection(t *testing.T) {
	clientGenerator := &defaultInfluxConnectionService{}
	_, err := clientGenerator.NewConnection(nil)
	if err == nil {
		t.Error("Should not be able to create a client without connection params")
	}

	serverParams := &InfluxConnectionParams{
		Server:   "",
		Username: "",
		Password: "",
	}

	_, err = clientGenerator.NewConnection(serverParams)
	if err == nil {
		t.Error("Server address should not be accepted")
	}

	serverParams.Server = "http://someaddress"
	influxClient, err := clientGenerator.NewConnection(serverParams)

	if err != nil || influxClient == nil {
		t.Error("Client should have been created without errors")
	}
}

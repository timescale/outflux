package pipeline

type ConnectionConfig struct {
	InputHost       string
	InputDb         string
	InputMeasures   []string
	InputUser       string
	InputPass       string
	OutputHost      string
	OutputDb        string
	OutputSchema    string
	OutputDbSslMode string
	OutputUser      string
	OutputPassword  string
}

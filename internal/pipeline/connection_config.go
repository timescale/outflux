package pipeline

type ConnectionConfig struct {
	InputHost          string
	InputDb            string
	InputMeasures      []string
	InputUser          string
	InputPass          string
	UseEnvVars         bool
	OutputDbConnString string
	OutputSchema       string
}

package pipeline

type ConnectionConfig struct {
	InputHost          string
	InputDb            string
	InputMeasures      []string
	InputUser          string
	InputPass          string
	OutputDbConnString string
	OutputSchema       string
}

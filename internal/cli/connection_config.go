package cli

// ConnectionConfig holds all arguments required to establish a connection to an input and output db
type ConnectionConfig struct {
	InputHost          string
	InputDb            string
	InputMeasures      []string
	InputUser          string
	InputPass          string
	InputUnsafeHTTPS   bool
	OutputDbConnString string
}

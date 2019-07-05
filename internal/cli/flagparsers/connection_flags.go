package flagparsers

import (
	"github.com/spf13/cobra"
)

// AddConnectionFlagsToCmd adds the flags required to connect to an Influx and Timescale database
func AddConnectionFlagsToCmd(cmd *cobra.Command) {
	cmd.PersistentFlags().String(
		InputServerFlag,
		DefaultInputServer,
		"Host of the input database, http(s)://location:port.")
	cmd.PersistentFlags().String(
		InputUserFlag,
		DefaultInputUser,
		"Username to use when connecting to the input database. If set overrides $INFLUX_USERNAME")
	cmd.PersistentFlags().String(
		InputPassFlag,
		DefaultInputPass,
		"Password to use when connecting to the input database. If set overrides $INFLUX_PASSWORD")
	cmd.PersistentFlags().Bool(
		InputUnsafeHTTPSFlag,
		DefaultInputUnsafeHTTPS,
		"Should 'InsecureSkipVerify' be passed to the input connection")
	cmd.PersistentFlags().String(
		OutputConnFlag,
		DefaultOutputConn,
		"Connection string to use to connect to the output database, overrides values in the PG environment variables")
	cmd.PersistentFlags().String(
		OutputSchemaFlag,
		DefaultOutputSchema,
		"The schema of the output database that the data will be inserted into")
}

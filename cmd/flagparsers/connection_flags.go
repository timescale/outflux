package flagparsers

import (
	"github.com/spf13/cobra"
)

// AddConnectionFlagsToCmd adds the flags required to connect to an Influx and Timescale database
func AddConnectionFlagsToCmd(cmd *cobra.Command) {
	cmd.PersistentFlags().String(InputHostFlag, DefaultInputHost, "Host of the input database, http(s)://location:port.")
	cmd.PersistentFlags().String(InputUserFlag, DefaultInputUser, "Username to use when connecting to the input database")
	cmd.PersistentFlags().String(InputPassFlag, DefaultInputPass, "Password to use when connecting to the input database")
	cmd.PersistentFlags().String(OutputHostFlag, DefaultOutputHost, "Host of the output database, location:port")
	cmd.PersistentFlags().String(OutputUserFlag, "", "Username to use when connecting to the output database")
	cmd.PersistentFlags().String(OutputPasswordFlag, "", "Password to use when connecting to the output database")
	cmd.PersistentFlags().String(OutputDbFlag, "", "Output (Target) database that the data will be inserted into")
	cmd.PersistentFlags().String(OutputSchemaFlag, "public", "The schema of the output database that the data will be inserted into")
	cmd.PersistentFlags().String(OutputDbSslModeFlag, "disable", "SSL mode to use when connecting to the output server. Valid: disable, require, verify-ca, verify-full")
}

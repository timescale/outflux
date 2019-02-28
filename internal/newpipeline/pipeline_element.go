package newpipeline

import (
	"github.com/timescale/outflux/internal/idrf"
)

type PipeElement interface {
	Prepare() error
	Run() error
	Input() chan idrf.Row
	Output() chan idrf.Row
	InputType() *idrf.DataSetInfo
	OutputType() *idrf.DataSetInfo
}

type PipeElementData interface
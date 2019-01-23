package api

import (
	"fmt"
	"testing"

	"github.com/timescale/outflux/extraction/config"
	"github.com/timescale/outflux/extraction/extractors"
	"github.com/timescale/outflux/schemadiscovery/clientutils"
)

type measureConf = config.MeasureExtraction
type connParams = clientutils.ConnectionParams
type extractorConf = config.ExtractorConfig

func TestCreateExtractors(t *testing.T) {
	cases := []struct {
		arg           extractorConf
		expectedError bool
		generator     extractors.InfluxExtractorGenerator
	}{
		{ //No measures, no extractors created, no error expected
			arg: extractorConf{
				Measures: []*measureConf{},
			},
			expectedError: false,
			generator:     returnErrorGenerator(),
		}, { // Extractor Generator returns an error, no result should be expected
			arg: extractorConf{
				Measures: []*measureConf{
					&measureConf{},
				},
			},
			expectedError: true,
			generator:     returnErrorGenerator(),
		}, { // Extractor generator returns a mock extractor which contains the passed measure conf
			arg: extractorConf{
				Measures: []*measureConf{&measureConf{}},
			},
			expectedError: false,
			generator:     okGenerator(),
		},
	}

	for _, testCase := range cases {
		api := defaultExtractorGenerator{generator: testCase.generator}

		result, err := api.CreateExtractors(&testCase.arg)
		errorReturned := err != nil
		if testCase.expectedError != errorReturned {
			t.Errorf("expected test case to return error: %v, Got errorReturned: %v", testCase.expectedError, errorReturned)
		}

		if testCase.expectedError {
			continue
		}

		numReturned := len(result)
		numExpected := len(testCase.arg.Measures)
		if numReturned != numExpected {
			t.Errorf("Num extractors expected: %d, got: %d", numExpected, numReturned)
		}
	}
}

type mockGenerator struct {
	res extractors.InfluxExtractor
	err error
}

func (gen *mockGenerator) Generate(
	config *config.MeasureExtraction, connection *clientutils.ConnectionParams,
) (extractors.InfluxExtractor, error) {
	return gen.res, gen.err
}

// A mock Extractor Generator that always returns an error if called
func returnErrorGenerator() extractors.InfluxExtractorGenerator {
	return &mockGenerator{nil, fmt.Errorf("generator returns error")}
}

// A generator that returns a mock extractor, the mock extractor contains the measurement
// config that was passed to the generator
func okGenerator() extractors.InfluxExtractorGenerator {
	return &mockGenerator{&mockExtractor{}, nil}
}

// Mock Extractor implementation
type mockExtractor struct {
}

func (e *mockExtractor) Start() (*extractors.ExtractedInfo, error) {
	return nil, nil
}

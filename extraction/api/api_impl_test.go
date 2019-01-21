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
		generator     extractors.GenerateExtractorFn
	}{
		{ //No measures, no extractors created, no error expected
			arg: extractorConf{
				Measures: []*measureConf{},
			},
			expectedError: false,
			generator:     failIfCalledGenerator(t),
		}, { // Extractor Generator returns an error, no result should be expected
			arg: extractorConf{
				Measures: []*measureConf{
					&measureConf{},
				},
			},
			expectedError: true,
			generator:     returnErrorGenerator,
		}, { // Extractor generator returns a mock extractor which contains the passed measure conf
			arg: extractorConf{
				Measures: []*measureConf{&measureConf{}},
			},
			expectedError: false,
			generator:     okGenerator,
		},
	}

	for _, testCase := range cases {
		api := defaultExtractorGenerator{generate: testCase.generator}

		result, err := api.CreateExtractors(&testCase.arg)
		errorReturned := err != nil
		if testCase.expectedError != errorReturned {
			t.Errorf("Expected test case to return error: %v, Got errorReturned: %v", testCase.expectedError, errorReturned)
		}

		if testCase.expectedError {
			continue
		}

		numReturned := len(result)
		numExpected := len(testCase.arg.Measures)
		if numReturned != numExpected {
			t.Errorf("Num extractors expected: %d, got: %d", numExpected, numReturned)
		}

		for index, expectedMeasureConf := range testCase.arg.Measures {
			returnedExtractor := result[index].(*mockExtractor)
			if expectedMeasureConf != returnedExtractor.calledWith {
				t.Errorf(
					"Returned extractor did not contain expected config. Expected: %v, got: %v",
					expectedMeasureConf,
					returnedExtractor.calledWith,
				)
			}
		}
	}
}

// Creates a mock Extractor Generator function that fails the test if called
func failIfCalledGenerator(t *testing.T) extractors.GenerateExtractorFn {
	return func(*measureConf, *connParams) (extractors.InfluxExtractor, error) {
		t.Errorf("extractor generator should not have been called")
		return nil, nil
	}
}

// A mock Extractor Generator that always returns an error if called
func returnErrorGenerator(*measureConf, *connParams) (extractors.InfluxExtractor, error) {
	return nil, fmt.Errorf("generator returns error")
}

// A generator that returns a mock extractor, the mock extractor contains the measurement
// config that was passed to the generator
func okGenerator(conf *measureConf, conn *connParams) (extractors.InfluxExtractor, error) {
	return &mockExtractor{calledWith: conf}, nil
}

// Mock Extractor implementation
type mockExtractor struct {
	calledWith *measureConf
}

func (e *mockExtractor) Start() (*extractors.ExtractedInfo, error) {
	return nil, nil
}

func (e *mockExtractor) Stop() error {
	return nil
}

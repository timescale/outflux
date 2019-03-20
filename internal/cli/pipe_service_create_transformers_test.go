package cli

import (
	"fmt"
	"testing"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/timescale/outflux/internal/idrf"
	"github.com/timescale/outflux/internal/transformation"
)

func TestCreateTransformers(t *testing.T) {
	err := fmt.Errorf("error")
	testCases := []struct {
		desc             string
		mock             *psctMockService
		expectedTransIds []string
		expectErr        bool
		conf             *MigrationConfig
		connConf         *ConnectionConfig
	}{
		{
			desc:      "error on tags as json transformer",
			mock:      &psctMockService{tagsErr: err},
			conf:      &MigrationConfig{TagsAsJSON: true},
			connConf:  &ConnectionConfig{},
			expectErr: true,
		}, {
			desc: "error on fields as json transformer",
			mock: &psctMockService{
				fieldsErr: err,
			},
			conf:      &MigrationConfig{FieldsAsJSON: true},
			connConf:  &ConnectionConfig{},
			expectErr: true,
		}, {
			desc:             "tags transformer is nil, no tags for measure",
			mock:             &psctMockService{renameT: &psctMockTrans{"r"}},
			expectedTransIds: []string{"r"},
			conf:             &MigrationConfig{TagsAsJSON: true},
			connConf:         &ConnectionConfig{},
		}, {
			desc: "all transformers created",
			mock: &psctMockService{
				tagsT:   &psctMockTrans{id: "t"},
				fieldsT: &psctMockTrans{id: "f"},
				renameT: &psctMockTrans{id: "r"},
			},
			expectedTransIds: []string{"t", "f", "r"},
			conf:             &MigrationConfig{FieldsAsJSON: true, TagsAsJSON: true},
			connConf:         &ConnectionConfig{},
		},
	}
	for _, tc := range testCases {
		ps := &pipeService{
			transformerService: tc.mock,
		}

		trans, err := ps.createTransformers("id", nil, "measure", tc.connConf, tc.conf)
		if err == nil && tc.expectErr {
			t.Fatalf("%s:expected error, none got", tc.desc)
		} else if err != nil && !tc.expectErr {
			t.Fatalf("%s: unexpected err: %v", tc.desc, err)
		}

		if tc.expectErr {
			continue
		}

		if len(trans) != len(tc.expectedTransIds) {
			t.Fatalf("%s: expected %d transformers, got %d", tc.desc, len(tc.expectedTransIds), len(trans))
		}

		for i, returnedTrans := range trans {
			if returnedTrans.ID() != tc.expectedTransIds[i] {
				t.Fatalf("%s: expected trans id '%s', got '%s'", tc.desc, returnedTrans.ID(), tc.expectedTransIds[i])
			}
		}
	}
}

type psctMockService struct {
	tagsT     transformation.Transformer
	tagsErr   error
	fieldsT   transformation.Transformer
	fieldsErr error
	renameT   transformation.Transformer
}

func (p *psctMockService) TagsAsJSON(infConn influx.Client, id, db, measure string, resultCol string) (transformation.Transformer, error) {
	return p.tagsT, p.tagsErr
}

func (p *psctMockService) FieldsAsJSON(infConn influx.Client, id, db, measure string, resultCol string) (transformation.Transformer, error) {
	return p.fieldsT, p.fieldsErr
}
func (p *psctMockService) RenameOutputSchema(id, outputSchema string) transformation.Transformer {
	return p.renameT
}

type psctMockTrans struct {
	id string
}

func (p *psctMockTrans) ID() string {
	return p.id
}
func (p *psctMockTrans) Prepare(input *idrf.Bundle) (*idrf.Bundle, error) { return nil, nil }
func (p *psctMockTrans) Start(chan error) error                           { return nil }

package webhookheaders_test

import (
	"testing"

	"github.com/block/schemabot/pkg/analyzers/webhookheaders"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, webhookheaders.Analyzer, "example")
}

// Command webhookheaders-check runs the webhookheaders analyzer as a
// standalone tool.
//
// Usage:
//
//	go run ./cmd/webhookheaders-check ./pkg/webhook/...
//
// The analyzer reports on every package it is given. The caller is
// responsible for excluding ./pkg/webhook/templates (the templates package
// is the legitimate home for `## ...` header strings).
package main

import (
	"github.com/block/schemabot/pkg/analyzers/webhookheaders"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(webhookheaders.Analyzer)
}

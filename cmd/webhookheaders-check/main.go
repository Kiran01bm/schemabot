// Command webhookheaders-check runs the webhookheaders analyzer as a
// standalone tool.
//
// Usage:
//
//	go run ./cmd/webhookheaders-check ./pkg/webhook/...
//
// The analyzer itself is generic — it does not know about pkg/webhook. The
// caller must pass the package set the rule should apply to (typically the
// webhook handler packages, excluding pkg/webhook/templates).
package main

import (
	"github.com/block/schemabot/pkg/analyzers/webhookheaders"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(webhookheaders.Analyzer)
}

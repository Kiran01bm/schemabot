// Package webhookheaders provides a go/analysis analyzer that flags inline
// markdown header strings (`## ...`) in Go source files. The project
// convention is for all PR-comment markdown to live in pkg/webhook/templates/
// and be rendered through a templates.Render… helper, so handler code that
// embeds a `## ...` header is almost always a missed extraction.
//
// The analyzer itself is generic — it does not know about pkg/webhook. The
// caller (Makefile / CI / pre-commit script) is responsible for invoking it
// only against the packages where the rule should apply.
package webhookheaders

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

const message = "inline markdown header `## ...` in handler code — move this body into pkg/webhook/templates and call templates.Render…"

// Analyzer flags string literals that begin with "## " in non-test source
// files. Test files are skipped because assertions on rendered comment bodies
// legitimately contain `## ...` substrings.
var Analyzer = &analysis.Analyzer{
	Name:     "webhookheaders",
	Doc:      "flags inline `## ...` markdown header strings; move them to pkg/webhook/templates and render via templates.Render…",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{(*ast.BasicLit)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		lit := n.(*ast.BasicLit)
		if lit.Kind != token.STRING {
			return
		}
		if isTestFile(pass, lit.Pos()) {
			return
		}
		val, err := strconv.Unquote(lit.Value)
		if err != nil {
			return
		}
		if strings.HasPrefix(val, "## ") {
			pass.Reportf(lit.Pos(), message)
		}
	})

	return nil, nil
}

func isTestFile(pass *analysis.Pass, pos token.Pos) bool {
	return strings.HasSuffix(pass.Fset.Position(pos).Filename, "_test.go")
}

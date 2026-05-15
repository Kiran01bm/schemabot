package example

// _test.go files are deliberately not flagged — assertions on rendered comment
// bodies legitimately contain `## ...` substrings and should not be moved into
// the templates package.

func unusedTestFixture() string {
	return "## Apply Blocked"
}

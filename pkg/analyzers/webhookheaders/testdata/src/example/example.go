package example

import "fmt"

// Bad: simple inline header.
func bad() string {
	return "## ❌ Apply Blocked\n\nDetails here." // want `inline markdown header`
}

// Bad: header concatenated with body.
func badConcat(name string) string {
	return "## Missing Argument\n\n" + // want `inline markdown header`
		"You must supply " + name + "."
}

// Bad: header inside fmt.Sprintf.
func badSprintf(env string) string {
	return fmt.Sprintf("## Apply Blocked\n\nEnvironment: %s", env) // want `inline markdown header`
}

// Bad: raw-string-literal header.
func badRaw() string {
	return `## Lock Held` // want `inline markdown header`
}

// Good: bold prefix is not a header.
func goodBold() string {
	return "**Repository not registered.** No `## ` markers up front."
}

// Good: header substring inside the string but not at the start.
func goodSubstring() string {
	return "see the section ## Configuration in the README"
}

// Good: hash without the required trailing space.
func goodNoSpace() string {
	return "##NotAHeader"
}

// Good: empty string.
func goodEmpty() string {
	return ""
}

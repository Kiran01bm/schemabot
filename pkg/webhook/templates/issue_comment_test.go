package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderRepositoryNotRegistered(t *testing.T) {
	rendered := RenderRepositoryNotRegistered()
	assert.Contains(t, rendered, "**Repository not registered.**")
	assert.Contains(t, rendered, "`repos`")
	assert.Contains(t, rendered, "redeploy")
}

func TestRenderRollbackMissingArguments(t *testing.T) {
	rendered := RenderRollbackMissingArguments()
	assert.Contains(t, rendered, "## Missing Arguments")
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e <environment>")
	assert.Contains(t, rendered, "both an apply ID and the `-e` flag")
}

func TestRenderRollbackMissingEnv(t *testing.T) {
	rendered := RenderRollbackMissingEnv()
	assert.Contains(t, rendered, "## Missing Environment")
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e <environment>")
	assert.Contains(t, rendered, "The `-e` flag is required")
	assert.NotContains(t, rendered, "both an apply ID",
		"missing-env variant should not say both args are missing")
}

func TestRenderUnsupportedAutoConfirm(t *testing.T) {
	rendered := RenderUnsupportedAutoConfirm("plan")
	assert.Equal(t, "The `-y` flag is not supported for `plan`.", rendered)
}

func TestRenderCommandNotYetAvailable(t *testing.T) {
	rendered := RenderCommandNotYetAvailable("stop", "staging")
	assert.Contains(t, rendered, "`stop` command is not yet available")
	assert.Contains(t, rendered, "Use the CLI instead")
	assert.Contains(t, rendered, "schemabot stop -e staging")
}

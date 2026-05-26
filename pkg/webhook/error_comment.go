package webhook

import (
	"github.com/block/schemabot/pkg/webhook/templates"
)

// postCommandError posts a generic command-failure comment to the PR with a
// consistent UTC timestamp.
//
// Centralized so handlers cannot accidentally omit Timestamp, which previously
// caused the comment footer to render as "Requested by @<user> at  UTC"
// (empty timestamp).
//
// Uses templates.NowFunc so previews and tests can substitute a deterministic
// clock — the same hook used by templates that already render UTC timestamps.
//
// Specialized error templates (RenderDatabaseNotFound, RenderInvalidConfig,
// RenderNoConfig, RenderMultipleConfigs, RenderReviewRequired,
// RenderUnsafeChangesBlocked, etc.) are intentionally not routed through this
// helper — they have additional fields or distinct rendering and remain
// constructed explicitly by their handlers.
func (h *Handler) postCommandError(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy, errorDetail string,
) {
	h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy: requestedBy,
		Timestamp:   templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment: environment,
		CommandName: commandName,
		ErrorDetail: errorDetail,
	}))
}

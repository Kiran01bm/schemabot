package state

// ApplyStateInfo holds presentation-neutral metadata for a canonical Apply state.
//
// Renderers (CLI, TUI, PR comments) reach for Label when they need a short
// human-readable name. Control-plane code reaches for Terminal and BranchSetup
// to make scheduling and gating decisions. Centralizing this metadata avoids
// the drift that occurs when each consumer maintains its own switch.
//
// Engine-specific or surface-specific copy (Title Case headers, color, emoji)
// is intentionally NOT stored here — those remain local to the rendering layer
// so this registry does not overfit to one presentation.
type ApplyStateInfo struct {
	// Label is the canonical human-readable display name (sentence case),
	// e.g. "Waiting for deploy", "Cutting over", "Retrying".
	Label string

	// Terminal is true for states where no further processing will occur.
	// FailedRetryable is NOT terminal — the scheduler may retry it.
	// Stopped IS terminal at the apply level (operators must explicitly resume).
	Terminal bool

	// BranchSetup is true for PlanetScale branch-lifecycle phases where
	// per-table progress is not yet meaningful (all tables are queued).
	// Used by the CLI and TUI to suppress the table list during setup.
	// Pending and WaitingForDeploy are included because the deploy hasn't
	// started yet.
	BranchSetup bool
}

// applyMetadata is the registry of metadata for every canonical Apply state.
// Every value of Apply.* must appear here. The metadata_test invariant
// enforces this so a newly added state cannot silently miss a label or
// classification.
var applyMetadata = map[string]ApplyStateInfo{
	Apply.Pending: {
		Label:       "Pending",
		BranchSetup: true,
	},
	Apply.Running: {
		Label: "Running",
	},
	Apply.WaitingForDeploy: {
		Label:       "Waiting for deploy",
		BranchSetup: true,
	},
	Apply.WaitingForCutover: {
		Label: "Waiting for cutover",
	},
	Apply.CuttingOver: {
		Label: "Cutting over",
	},
	Apply.RevertWindow: {
		Label: "Revert window",
	},
	Apply.Completed: {
		Label:    "Completed",
		Terminal: true,
	},
	Apply.Failed: {
		Label:    "Failed",
		Terminal: true,
	},
	Apply.FailedRetryable: {
		Label: "Retrying",
	},
	Apply.Stopped: {
		Label:    "Stopped",
		Terminal: true,
	},
	Apply.Cancelled: {
		Label:    "Cancelled",
		Terminal: true,
	},
	Apply.Reverted: {
		Label:    "Reverted",
		Terminal: true,
	},
	Apply.PreparingBranch: {
		Label:       "Preparing branch",
		BranchSetup: true,
	},
	Apply.ApplyingBranchChanges: {
		Label:       "Applying changes to branch",
		BranchSetup: true,
	},
	Apply.ValidatingBranch: {
		Label:       "Validating branch",
		BranchSetup: true,
	},
	Apply.CreatingDeployRequest: {
		Label:       "Creating deploy request",
		BranchSetup: true,
	},
	Apply.ValidatingDeployRequest: {
		Label:       "Validating deploy request",
		BranchSetup: true,
	},
}

// LookupApply returns the metadata for the given Apply state and whether the
// state is known to the registry. Unknown states return the zero ApplyStateInfo
// and false; callers decide whether to fall back to the raw state string or
// treat the unknown state as an error.
func LookupApply(s string) (ApplyStateInfo, bool) {
	info, ok := applyMetadata[s]
	return info, ok
}

// Label returns the canonical human-readable label for an Apply state, or the
// state string itself when the state is not in the registry. Used by CLI, TUI,
// and PR templates where a short label is needed; surface-specific titles
// (e.g. Title Case PR headers) remain local to the rendering layer.
func Label(s string) string {
	if info, ok := applyMetadata[s]; ok {
		return info.Label
	}
	return s
}

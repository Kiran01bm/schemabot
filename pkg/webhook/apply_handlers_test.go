package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func TestFilterFailingNonSchemaBotChecks(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []ghclient.PRCheckStatus
		wantLen   int
		wantNames []string
	}{
		{
			name:     "empty statuses returns nil",
			statuses: nil,
			wantLen:  0,
		},
		{
			name: "all passing checks returns no failures",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "success"},
				{Name: "CI / lint", Status: "completed", Conclusion: "success"},
			},
			wantLen: 0,
		},
		{
			name: "failure is caught",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "failure"},
				{Name: "CI / lint", Status: "completed", Conclusion: "success"},
			},
			wantLen:   1,
			wantNames: []string{"CI / unit-tests"},
		},
		{
			name: "error and timed_out are caught",
			statuses: []ghclient.PRCheckStatus{
				{Name: "security-scan", Status: "completed", Conclusion: "error"},
				{Name: "CI / integration", Status: "completed", Conclusion: "timed_out"},
			},
			wantLen:   2,
			wantNames: []string{"security-scan", "CI / integration"},
		},
		{
			name: "SchemaBot checks are excluded",
			statuses: []ghclient.PRCheckStatus{
				{Name: "SchemaBot Apply: /mysql/payments", Status: "completed", Conclusion: "failure", IsSchemaBot: true},
				{Name: "SchemaBot (staging)", Status: "completed", Conclusion: "failure", IsSchemaBot: true},
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "failure"},
			},
			wantLen:   1,
			wantNames: []string{"CI / unit-tests"},
		},
		{
			name: "neutral and skipped are ignored",
			statuses: []ghclient.PRCheckStatus{
				{Name: "informational-check", Status: "completed", Conclusion: "neutral"},
				{Name: "optional-check", Status: "completed", Conclusion: "skipped"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
			},
			wantLen:   1,
			wantNames: []string{"CI / lint"},
		},
		{
			name: "in-progress checks are not considered failing",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "in_progress", Conclusion: ""},
				{Name: "CI / lint", Status: "queued", Conclusion: ""},
			},
			wantLen: 0,
		},
		{
			name: "mixed statuses",
			statuses: []ghclient.PRCheckStatus{
				{Name: "CI / unit-tests", Status: "completed", Conclusion: "success"},
				{Name: "CI / lint", Status: "completed", Conclusion: "failure"},
				{Name: "CI / integration", Status: "in_progress", Conclusion: ""},
				{Name: "SchemaBot Apply: /mysql/db", Status: "completed", Conclusion: "action_required", IsSchemaBot: true},
				{Name: "optional", Status: "completed", Conclusion: "neutral"},
				{Name: "security", Status: "completed", Conclusion: "error"},
			},
			wantLen:   2,
			wantNames: []string{"CI / lint", "security"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failing := filterFailingNonSchemaBotChecks(tt.statuses)
			require.Len(t, failing, tt.wantLen)
			for i, name := range tt.wantNames {
				assert.Equal(t, name, failing[i].Name)
			}
		})
	}
}

func TestDDLMatchesStoredPlan(t *testing.T) {
	tests := []struct {
		name       string
		planResp   *apitypes.PlanResponse
		storedPlan *storage.Plan
		wantMatch  bool
	}{
		{
			name: "identical DDL matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "different DDL does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{DDL: "ALTER TABLE `users` DROP COLUMN `old_field`"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "different DDL content does not match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(500)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			wantMatch: false,
		},
		{
			name: "same DDL in different order matches",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{
					{TableChanges: []*apitypes.TableChangeResponse{
						{DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
					}},
				},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{
					"mydb": {Tables: []storage.TableChange{
						{DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
						{DDL: "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)"},
					}},
				},
			},
			wantMatch: true,
		},
		{
			name: "empty plans match",
			planResp: &apitypes.PlanResponse{
				Changes: []*apitypes.SchemaChangeResponse{},
			},
			storedPlan: &storage.Plan{
				Namespaces: map[string]*storage.NamespacePlanData{},
			},
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMatch, ddlMatchesStoredPlan(tt.planResp, tt.storedPlan))
		})
	}
}

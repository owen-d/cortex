package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/rules"
)

type ManagerCreator interface {
	NewManager(context.Context, string) (UserManager, error)
}

type ManagerCreatorFunc func(context.Context, string) (UserManager, error)

func (fn ManagerCreatorFunc) NewManager(ctx context.Context, userID string) (UserManager, error) {
	return fn(ctx, userID)
}

type UserManager interface {
	Run()
	Stop()
	Update(interval time.Duration, files []string, externalLabels labels.Labels) error
	RuleGroups() []Group
}

type managerAdapter struct {
	*rules.Manager
}

func NewManagerAdapter(mgr *rules.Manager) UserManager {
	return &managerAdapter{mgr}
}

func (m *managerAdapter) RuleGroups() []Group {
	grps := m.Manager.RuleGroups()
	result := make([]Group, 0, len(grps))
	for _, g := range grps {
		result = append(result, g)
	}
	return result

}

type Group interface {
	Name() string
	File() string
	Interval() time.Duration
	GetEvaluationTimestamp() time.Time
	GetEvaluationDuration() time.Duration
	Rules() []rules.Rule
}

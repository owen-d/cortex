package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	promRules "github.com/prometheus/prometheus/rules"
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
	RuleGroups() []*promRules.Group
}

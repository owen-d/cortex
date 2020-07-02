package ruler

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/user"
)

func PrometheusManagerCreator(r *Ruler) ManagerCreator {
	return ManagerCreatorFunc(func(ctx context.Context, notifier *notifier.Manager, userID string) (UserManager, error) {
		// Wrap registerer with userID and cortex_ prefix
		reg := prometheus.WrapRegistererWith(prometheus.Labels{"user": userID}, r.registry)
		reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
		logger := log.With(r.logger, "user", userID)
		opts := &promRules.ManagerOptions{
			Appendable:      &appender{pusher: r.pusher, userID: userID},
			Queryable:       r.queryable,
			QueryFunc:       engineQueryFunc(r.engine, r.queryable, r.cfg.EvaluationDelay),
			Context:         user.InjectOrgID(ctx, userID),
			ExternalURL:     r.alertURL,
			NotifyFunc:      SendAlerts(notifier, r.alertURL.String()),
			Logger:          logger,
			Registerer:      reg,
			OutageTolerance: r.cfg.OutageTolerance,
			ForGracePeriod:  r.cfg.ForGracePeriod,
			ResendDelay:     r.cfg.ResendDelay,
		}
		return promRules.NewManager(opts), nil
	})
}

type ManagerCreator interface {
	NewManager(context.Context, *notifier.Manager, string) (UserManager, error)
}

type ManagerCreatorFunc func(context.Context, *notifier.Manager, string) (UserManager, error)

func (fn ManagerCreatorFunc) NewManager(ctx context.Context, notifier *notifier.Manager, userID string) (UserManager, error) {
	return fn(ctx, notifier, userID)
}

type UserManager interface {
	Run()
	Stop()
	Update(interval time.Duration, files []string, externalLabels labels.Labels) error
	RuleGroups() []*promRules.Group
}

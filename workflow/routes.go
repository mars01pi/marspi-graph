package workflow

import (
	"context"
	"fmt"

	"github.com/mars/marspi-graph/graph"
)

func buildRoute(e EdgeSpec) (graph.RouteFunc, error) {
	switch e.Route {
	case RoutePassFail:
		key, pass, fail := e.Key, e.Pass, e.Fail
		return func(_ context.Context, s graph.State) string {
			if isPass(s.GetString(key)) {
				return normalizeTarget(pass)
			}
			return normalizeTarget(fail)
		}, nil
	case RoutePassFailRework:
		key, pass, fail := e.Key, e.Pass, e.Fail
		countKey, maxKey := e.CountKey, e.MaxKey
		exhausted := e.Exhausted
		if exhausted == "" {
			exhausted = "END"
		}
		return func(_ context.Context, s graph.State) string {
			if isPass(s.GetString(key)) {
				return normalizeTarget(pass)
			}
			count := stateInt(s, countKey)
			max := stateInt(s, maxKey)
			if max > 0 && count > max {
				return normalizeTarget(exhausted)
			}
			return normalizeTarget(fail)
		}, nil
	case RouteApproval:
		approved, denied := e.Approved, e.Denied
		return func(_ context.Context, s graph.State) string {
			if approvalTruthy(s["approval"]) {
				return normalizeTarget(approved)
			}
			return normalizeTarget(denied)
		}, nil
	default:
		return nil, fmt.Errorf("workflow: unknown route %q", e.Route)
	}
}

func normalizeTarget(to string) string {
	if to == "END" || to == "" {
		return graph.END
	}
	if to == "START" {
		return graph.START
	}
	return to
}

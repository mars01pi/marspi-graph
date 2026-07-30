package component

import (
	"context"
	"fmt"
	"strings"

	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/graph"
)

// Agent is the code entity for ReAct coding/review agents.
type Agent struct {
	cfg agentConfig
}

type agentConfig struct {
	SystemPrompt string            `yaml:"system_prompt"`
	UserPrompt   string            `yaml:"user_prompt"`
	AllowTools   []string          `yaml:"allow_tools"`
	Persist      string            `yaml:"persist"`
	Spawn        []string          `yaml:"spawn"`
	WriteState   map[string]string `yaml:"write_state"`
	VerdictKey   string            `yaml:"verdict_key"`
	Extractors   []ExtractorSpec   `yaml:"extractors"`
	Outputs      []OutputSpec      `yaml:"outputs"`
}

// NewAgent returns an unbound Agent component.
func NewAgent() *Agent { return &Agent{} }

func (a *Agent) Meta() Meta {
	return Meta{Type: TypeAgent, Name: "Agent", Description: "ReAct agent with tools"}
}

func (a *Agent) TypeSchema() Schema {
	return Schema{
		Meta: a.Meta(),
		Inputs: []InputDef{
			{Name: "system_prompt", Label: "系统提示", Types: []string{"str"}, Widget: "textarea"},
			{Name: "user_prompt", Label: "用户提示", Types: []string{"str"}, Widget: "textarea", Advanced: true},
			{Name: "allow_tools", Label: "工具", Types: []string{"list"}, Widget: "multiselect", OptionsSource: "tools"},
			{Name: "persist", Label: "会话持久化", Types: []string{"str"}, Default: "none"},
			{Name: "spawn", Label: "可 Spawn 子代理", Types: []string{"list"}, Advanced: true},
			{Name: "write_state", Label: "写入 State", Types: []string{"object"}, Advanced: true},
			{Name: "verdict_key", Label: "裁决字段（已弃用，改用 extractors completion）", Types: []string{"str"}, Advanced: true},
			{Name: "extractors", Label: "Extractors", Types: []string{"list"}, Advanced: true},
			{Name: "outputs", Label: "对外输出", Types: []string{"list"}, Advanced: true},
		},
		Outputs: []OutputDef{
			{Name: "output", Label: "回复内容", Types: []string{"str"}},
		},
	}
}

func (a *Agent) Bind(bindings map[string]any) error {
	return decodeBindings(bindings, &a.cfg)
}

func (a *Agent) InstanceSchema() Schema {
	sch := a.TypeSchema()
	for _, o := range a.effectiveOutputs() {
		sch.Outputs = append(sch.Outputs, OutputDef{
			Name:  o.Name,
			Label: firstNonEmpty(o.Label, o.Name),
			Types: []string{"str"},
		})
	}
	return sch
}

func (a *Agent) effectiveOutputs() []OutputSpec {
	byName := map[string]OutputSpec{}
	for _, o := range a.cfg.Outputs {
		name := strings.TrimSpace(o.Name)
		if name == "" {
			continue
		}
		o.Name = name
		byName[name] = o
	}
	add := func(name, sk, label string) {
		if name == "" {
			return
		}
		if _, ok := byName[name]; ok {
			return
		}
		byName[name] = OutputSpec{Name: name, StateKey: sk, Label: label}
	}
	for k := range a.cfg.WriteState {
		add(k, k, k)
	}
	if a.cfg.VerdictKey != "" {
		add(a.cfg.VerdictKey, a.cfg.VerdictKey, a.cfg.VerdictKey)
	}
	for _, ex := range a.cfg.Extractors {
		add(ex.To, ex.To, ex.To)
	}
	out := make([]OutputSpec, 0, len(byName))
	for _, o := range byName {
		out = append(out, o)
	}
	return out
}

// PublishedOutputs returns instance outputs for workflow ref catalog.
func (a *Agent) PublishedOutputs() []OutputSpec { return a.effectiveOutputs() }

func (a *Agent) BuildNode(rt Runtime) (graph.NodeFunc, error) {
	deps := depsFromHost(rt.Host)
	if deps == nil || deps.Provider == nil {
		return nil, fmt.Errorf("component agent: ExecDeps.Provider required")
	}
	cfg := a.cfg
	nodeID := rt.NodeID
	failInc := rt.FailIncKey
	failCheck := rt.FailCheckKey
	expand := rt.ExpandRefs

	return func(runCtx context.Context, s graph.State) (graph.Update, error) {
		sys := cfg.SystemPrompt
		if sys == "" {
			sys = deps.SystemPrompt
		} else if deps.SystemPrompt != "" {
			sys = deps.SystemPrompt + "\n\n" + sys
		}
		if expand != nil {
			sys = expand(sys, s, nil)
		}

		reg := deps.Registry
		if reg != nil && len(cfg.Spawn) > 0 {
			reg = cloneRegistryWithSpawn(reg, deps, cfg.Spawn)
		}

		spec := agentspec.Spec{
			ID:           nodeID,
			SystemPrompt: sys,
			Provider:     deps.Provider,
			Registry:     reg,
			AllowTools:   cfg.AllowTools,
			MaxContext:   deps.MaxContext,
			MaxIter:      deps.MaxIterAgent,
			Stream:       deps.Stream,
			Reporter:     deps.Reporter,
			Events:       deps.Events,
		}
		if cfg.Persist == "session" && deps.Durable != nil {
			spec.Persist = agentspec.PersistSession
			spec.Store = deps.Durable
		}
		inst := agentspec.New(spec)
		prompt := buildAgentUserPrompt(cfg, s, expand)
		out, err := inst.RunOnce(runCtx, prompt)
		if err != nil {
			return nil, fmt.Errorf("component agent %q: %w", nodeID, err)
		}

		upd := graph.Update{
			"phase":      nodeID,
			"last_agent": nodeID,
			"messages":   fmt.Sprintf("[%s] %s", nodeID, truncate(out, 400)),
		}
		extras := map[string]string{"output": out, "goal": s.GetString("goal")}
		for key, tmpl := range cfg.WriteState {
			if expand != nil {
				upd[key] = expand(tmpl, s, extras)
			} else {
				upd[key] = tmpl
			}
		}
		extractors := withVerdictCompletion(cfg.Extractors, cfg.VerdictKey)
		applyExtractors(extractors, inst.Manager(), out, upd)
		applyFailInc(upd, s, failInc, failCheck, cfg.VerdictKey)
		return upd, nil
	}, nil
}

// withVerdictCompletion treats deprecated verdict_key as a completion extractor.
func withVerdictCompletion(extractors []ExtractorSpec, verdictKey string) []ExtractorSpec {
	key := strings.TrimSpace(verdictKey)
	if key == "" {
		return extractors
	}
	for _, ex := range extractors {
		if ex.From == ExtractCompletion && strings.TrimSpace(ex.To) == key {
			return extractors
		}
	}
	out := make([]ExtractorSpec, 0, len(extractors)+1)
	out = append(out, ExtractorSpec{To: key, From: ExtractCompletion})
	return append(out, extractors...)
}

func applyFailInc(upd graph.Update, s graph.State, failInc, failCheck, verdictKey string) {
	if failInc == "" {
		return
	}
	check := failCheck
	if check == "" {
		check = verdictKey
	}
	if check == "" {
		return
	}
	verdict, _ := upd[check].(string)
	if verdict == "" {
		verdict = s.GetString(check)
	}
	if isFail(verdict) {
		upd[failInc] = stateInt(s, failInc) + 1
	}
}

func buildAgentUserPrompt(cfg agentConfig, s graph.State, expand RefExpander) string {
	if strings.TrimSpace(cfg.UserPrompt) != "" {
		if expand != nil {
			return expand(cfg.UserPrompt, s, nil)
		}
		return cfg.UserPrompt
	}
	var b strings.Builder
	fmt.Fprintf(&b, "GOAL: %s\n", s.GetString("goal"))
	fmt.Fprintf(&b, "PHASE\n\n")
	if d := s.GetString("design_doc"); d != "" {
		fmt.Fprintf(&b, "DESIGN_DOC:\n%s\n\n", d)
	}
	if f := s.GetString("changed_files"); f != "" {
		fmt.Fprintf(&b, "CHANGED_FILES:\n%s\n\n", f)
	}
	if r := s.GetString("review_result"); r != "" {
		fmt.Fprintf(&b, "REVIEW_RESULT: %s\n\n", r)
	}
	if t := s.GetString("test_result"); t != "" {
		fmt.Fprintf(&b, "TEST_RESULT: %s\n\n", t)
	}
	if cfg.VerdictKey != "" {
		fmt.Fprintf(&b, "When done, call attempt_completion with a one-line verdict containing PASS or FAIL.\n")
	} else {
		fmt.Fprintf(&b, "Call attempt_completion when this phase is done.\n")
	}
	return b.String()
}

func cloneRegistryWithSpawn(parent *tool.Registry, deps *ExecDeps, spawnIDs []string) *tool.Registry {
	children := resolveSpawnChildren(deps, spawnIDs)
	if len(children) == 0 {
		return parent
	}
	if _, ok := parent.Get("spawn"); ok {
		return parent
	}
	_ = agentspec.RegisterSpawn(parent, deps.Provider, agentspec.SpawnConfig{Children: children})
	return parent
}

func resolveSpawnChildren(deps *ExecDeps, ids []string) []agentspec.ChildSpec {
	byID := map[string]agentspec.ChildSpec{}
	for _, c := range deps.SpawnChildren {
		byID[c.ID] = c
	}
	var out []agentspec.ChildSpec
	for _, id := range ids {
		if c, ok := byID[id]; ok {
			out = append(out, c)
			continue
		}
		if id == "explore" {
			out = append(out, agentspec.ChildSpec{
				ID:           "explore",
				Description:  "Read/search codebase; do not edit",
				SystemPrompt: "You are an explore scout. Use read/grep/search only. Summarize findings.",
				AllowTools:   []string{"read", "grep", "search", "attempt_completion"},
			})
		}
	}
	return out
}

func isFail(verdict string) bool {
	return strings.Contains(strings.ToUpper(verdict), "FAIL")
}

func stateInt(s graph.State, key string) int {
	switch v := s[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

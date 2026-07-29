package agentspec

import (
	"context"
	"fmt"
	"strings"

	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
)

const (
	spawnToolName      = "spawn"
	defaultSpawnDepth  = 1
	defaultSpawnOutput = 8 * 1024
	spawnTruncMarker   = "\n…[truncated]"
)

type spawnDepthKey struct{}

// ChildSpec describes one specialized child agent that the parent may spawn.
type ChildSpec struct {
	ID           string
	Description  string // shown in spawn tool description for routing
	SystemPrompt string
	Provider     llm.Provider // nil → inherit parentProvider from RegisterSpawn
	Registry     *tool.Registry
	AllowTools   []string
	MaxIter      int
}

// SpawnConfig configures the parent-side spawn tool.
type SpawnConfig struct {
	Children  []ChildSpec
	MaxDepth  int // default 1; children are not given spawn at this depth
	MaxOutput int // summary truncation; default 8KiB
}

// RegisterSpawn adds a ContextualTool named "spawn" to parent.
// parentProvider is used when a ChildSpec has a nil Provider.
func RegisterSpawn(parent *tool.Registry, parentProvider llm.Provider, cfg SpawnConfig) error {
	if parent == nil {
		return fmt.Errorf("agentspec: RegisterSpawn requires parent registry")
	}
	if len(cfg.Children) == 0 {
		return fmt.Errorf("agentspec: RegisterSpawn requires at least one child")
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = defaultSpawnDepth
	}
	if cfg.MaxOutput <= 0 {
		cfg.MaxOutput = defaultSpawnOutput
	}

	byID := make(map[string]ChildSpec, len(cfg.Children))
	var descParts []string
	for _, c := range cfg.Children {
		id := strings.TrimSpace(c.ID)
		if id == "" {
			return fmt.Errorf("agentspec: child id must be non-empty")
		}
		if id == spawnToolName {
			return fmt.Errorf("agentspec: child id %q is reserved", spawnToolName)
		}
		if _, ok := byID[id]; ok {
			return fmt.Errorf("agentspec: duplicate child id %q", id)
		}
		c.ID = id
		if c.Provider == nil && parentProvider == nil {
			return fmt.Errorf("agentspec: child %q needs Provider or parentProvider", id)
		}
		byID[id] = c
		label := c.Description
		if label == "" {
			label = id
		}
		descParts = append(descParts, fmt.Sprintf("- %s: %s", id, label))
	}

	t := &spawnTool{
		children:       byID,
		parentProvider: parentProvider,
		maxDepth:       cfg.MaxDepth,
		maxOutput:      cfg.MaxOutput,
		typeEnum:       childIDs(cfg.Children),
		descExtra:      strings.Join(descParts, "\n"),
	}
	parent.Register(t)
	return nil
}

func childIDs(children []ChildSpec) []string {
	out := make([]string, 0, len(children))
	for _, c := range children {
		out = append(out, strings.TrimSpace(c.ID))
	}
	return out
}

// SpawnDepth returns the current spawn nesting depth (0 = parent / top-level).
func SpawnDepth(ctx context.Context) int {
	if v, ok := ctx.Value(spawnDepthKey{}).(int); ok {
		return v
	}
	return 0
}

// WithSpawnDepth returns a child context carrying spawn nesting depth.
// RegisterSpawn sets this automatically; tests may inject depth directly.
func WithSpawnDepth(ctx context.Context, depth int) context.Context {
	return context.WithValue(ctx, spawnDepthKey{}, depth)
}

type spawnTool struct {
	tool.Base
	children       map[string]ChildSpec
	parentProvider llm.Provider
	maxDepth       int
	maxOutput      int
	typeEnum       []string
	descExtra      string
}

func (t *spawnTool) Name() string { return spawnToolName }

func (t *spawnTool) Description() string {
	return "Delegate a focused subtask to a specialized child agent with an isolated context. " +
		"The child does not see the parent conversation; put all needed detail in task. " +
		"Available agents:\n" + t.descExtra
}

func (t *spawnTool) Params() []tool.Param {
	enumHint := strings.Join(t.typeEnum, "|")
	return []tool.Param{
		{
			Name: "agent_type",
			Type: "string",
			Desc: "Child agent id (" + enumHint + ")",
		},
		{
			Name: "task",
			Type: "string",
			Desc: "Self-contained instructions for the child",
		},
		{
			Name: "description",
			Type: "string?",
			Desc: "Short 3-5 word label for this spawn",
		},
	}
}

func (t *spawnTool) UseSpinner() bool { return true }

func (t *spawnTool) Preview(args map[string]any) string {
	typ, _ := args["agent_type"].(string)
	desc, _ := args["description"].(string)
	if desc != "" {
		return typ + ": " + desc
	}
	task, _ := args["task"].(string)
	return typ + ": " + truncateRunes(task, 80)
}

func (t *spawnTool) Run(args map[string]any) tool.Result {
	return t.RunCtx(context.Background(), tool.ExecutionMeta{}, args)
}

func (t *spawnTool) RunCtx(ctx context.Context, _ tool.ExecutionMeta, args map[string]any) tool.Result {
	if err := ctx.Err(); err != nil {
		return tool.Fail("spawn canceled: " + err.Error())
	}
	depth := SpawnDepth(ctx)
	if depth >= t.maxDepth {
		return tool.Fail(fmt.Sprintf("spawn depth exceeded (%d >= %d)", depth, t.maxDepth))
	}

	agentType, _ := args["agent_type"].(string)
	agentType = strings.TrimSpace(agentType)
	task, _ := args["task"].(string)
	task = strings.TrimSpace(task)
	if agentType == "" {
		return tool.Fail("spawn missing agent_type")
	}
	if task == "" {
		return tool.Fail("spawn missing task")
	}
	child, ok := t.children[agentType]
	if !ok {
		return tool.Fail(fmt.Sprintf("spawn unknown agent_type %q", agentType))
	}

	provider := child.Provider
	if provider == nil {
		provider = t.parentProvider
	}
	maxIter := child.MaxIter
	if maxIter <= 0 {
		maxIter = 32
	}

	inst := New(Spec{
		ID:           child.ID,
		SystemPrompt: child.SystemPrompt,
		Provider:     provider,
		Registry:     child.Registry,
		AllowTools:   child.AllowTools,
		MaxIter:      maxIter,
		Persist:      PersistNone,
	})

	childCtx := WithSpawnDepth(ctx, depth+1)
	out, err := inst.RunOnce(childCtx, task)
	if err != nil {
		if out != "" {
			return tool.Fail(fmt.Sprintf("spawn %q: %v\npartial: %s", child.ID, err, truncateOutput(out, t.maxOutput)))
		}
		return tool.Fail(fmt.Sprintf("spawn %q: %v", child.ID, err))
	}
	return tool.OK(truncateOutput(out, t.maxOutput))
}

func truncateOutput(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	keep := max - len(spawnTruncMarker)
	if keep < 1 {
		keep = max
		return s[:keep]
	}
	return s[:keep] + spawnTruncMarker
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

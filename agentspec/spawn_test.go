package agentspec_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mars/marspi-core/config"
	"github.com/mars/marspi-core/console"
	"github.com/mars/marspi-core/llm"
	"github.com/mars/marspi-core/tool"
	"github.com/mars/marspi-graph/agentspec"
	"github.com/mars/marspi-graph/graph"
)

func TestRegisterSpawnSuccess(t *testing.T) {
	reg := emptyRegistry(t)
	err := agentspec.RegisterSpawn(reg, llm.NewProvider("gpt-4o", "http://127.0.0.1:1", "k"), agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{
			{ID: "explore", Description: "codebase scout"},
			{ID: "reviewer", Description: "diff review"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("spawn"); !ok {
		t.Fatal("spawn not registered")
	}
	var found bool
	for _, sch := range reg.Schemas() {
		fn, _ := sch["function"].(map[string]any)
		if fn["name"] != "spawn" {
			continue
		}
		found = true
		desc, _ := fn["description"].(string)
		if !strings.Contains(desc, "explore") || !strings.Contains(desc, "reviewer") {
			t.Fatalf("description missing agent ids: %q", desc)
		}
		params, _ := fn["parameters"].(map[string]any)
		props, _ := params["properties"].(map[string]any)
		if _, ok := props["agent_type"]; !ok {
			t.Fatal("missing agent_type")
		}
		if _, ok := props["task"]; !ok {
			t.Fatal("missing task")
		}
		agentType, _ := props["agent_type"].(map[string]any)
		adesc, _ := agentType["description"].(string)
		if !strings.Contains(adesc, "explore") || !strings.Contains(adesc, "reviewer") {
			t.Fatalf("agent_type desc missing enum hint: %q", adesc)
		}
	}
	if !found {
		t.Fatal("spawn schema missing")
	}
}

func TestRegisterSpawnValidation(t *testing.T) {
	reg := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", "http://127.0.0.1:1", "k")
	if err := agentspec.RegisterSpawn(reg, prov, agentspec.SpawnConfig{}); err == nil {
		t.Fatal("expected empty children error")
	}
	if err := agentspec.RegisterSpawn(nil, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "a"}},
	}); err == nil {
		t.Fatal("expected nil parent error")
	}
	if err := agentspec.RegisterSpawn(reg, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "a"}, {ID: "a"}},
	}); err == nil {
		t.Fatal("expected duplicate error")
	}
	if err := agentspec.RegisterSpawn(reg, nil, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "a"}},
	}); err == nil {
		t.Fatal("expected missing provider error")
	}
	if err := agentspec.RegisterSpawn(reg, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "spawn"}},
	}); err == nil {
		t.Fatal("expected reserved id error")
	}
}

func TestSpawnSyncSuccess(t *testing.T) {
	var sawTask atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		msgs, _ := req["messages"].([]any)
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			if mm["role"] == "user" {
				if c, _ := mm["content"].(string); strings.Contains(c, "find auth") {
					sawTask.Store(true)
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "mock",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "auth in login.go"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	if err := agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore", SystemPrompt: "You explore code."}},
	}); err != nil {
		t.Fatal(err)
	}
	res := runSpawn(t, parent, map[string]any{
		"agent_type": "explore",
		"task":       "find auth",
	})
	if !res.Success {
		t.Fatalf("result=%+v", res)
	}
	if res.Content != "auth in login.go" {
		t.Fatalf("content=%v", res.Content)
	}
	if !sawTask.Load() {
		t.Fatal("child did not see task")
	}
}

func TestSpawnUnknownAgentType(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "x"},
			}},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	_ = agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore"}},
	})
	res := runSpawn(t, parent, map[string]any{
		"agent_type": "missing",
		"task":       "hi",
	})
	if res.Success {
		t.Fatal("expected failure")
	}
	if hits.Load() != 0 {
		t.Fatal("child provider should not run")
	}
}

func TestSpawnContextIsolation(t *testing.T) {
	var mu sync.Mutex
	var childMsgs []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		msgs, _ := req["messages"].([]any)
		mu.Lock()
		for _, m := range msgs {
			if mm, ok := m.(map[string]any); ok {
				childMsgs = append(childMsgs, mm)
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
		})
	}))
	defer srv.Close()

	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	parentInst := agentspec.New(agentspec.Spec{
		ID:           "parent",
		SystemPrompt: "parent system",
		Provider:     prov,
		MaxIter:      1,
	})
	parentInst.Manager().AppendUser("secret parent history should not leak")
	parentInst.Manager().AppendAssistant(llm.Message{"role": "assistant", "content": "parent reply"})

	parentReg := emptyRegistry(t)
	if err := agentspec.RegisterSpawn(parentReg, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{
			{ID: "explore", SystemPrompt: "child system only"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	_ = parentInst // parent history exists but spawn must not reuse it
	res := runSpawn(t, parentReg, map[string]any{
		"agent_type": "explore",
		"task":       "child-only-task",
	})
	if !res.Success {
		t.Fatalf("%+v", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(childMsgs) == 0 {
		t.Fatal("no child messages")
	}
	for _, m := range childMsgs {
		c, _ := m["content"].(string)
		if strings.Contains(c, "secret parent history") || strings.Contains(c, "parent reply") {
			t.Fatalf("parent history leaked: %+v", childMsgs)
		}
	}
	joined := ""
	for _, m := range childMsgs {
		c, _ := m["content"].(string)
		joined += c + "\n"
	}
	if !strings.Contains(joined, "child-only-task") {
		t.Fatalf("missing task: %+v", childMsgs)
	}
}

func TestSpawnAllowTools(t *testing.T) {
	cfg := &config.Config{ProjectRoot: t.TempDir()}
	full := tool.NewRegistry(cfg, console.Nop{}, nil, nil)
	names := full.Names()
	hasRead, hasWrite := false, false
	for _, n := range names {
		if n == "read" {
			hasRead = true
		}
		if n == "write" {
			hasWrite = true
		}
	}
	if !hasRead || !hasWrite {
		t.Fatalf("expected read/write builtins, got %v", names)
	}

	var toolNames atomic.Value
	toolNames.Store([]string(nil))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		tools, _ := req["tools"].([]any)
		var got []string
		for _, tl := range tools {
			tm, _ := tl.(map[string]any)
			fn, _ := tm["function"].(map[string]any)
			if n, _ := fn["name"].(string); n != "" {
				got = append(got, n)
			}
		}
		toolNames.Store(got)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "done"},
			}},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	if err := agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{
			ID:         "reader",
			Registry:   full,
			AllowTools: []string{"read"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	res := runSpawn(t, parent, map[string]any{"agent_type": "reader", "task": "list"})
	if !res.Success {
		t.Fatalf("%+v", res)
	}
	got, _ := toolNames.Load().([]string)
	for _, n := range got {
		if n == "write" {
			t.Fatalf("write should be filtered, got %v", got)
		}
	}
	foundRead := false
	for _, n := range got {
		if n == "read" {
			foundRead = true
		}
	}
	if !foundRead {
		t.Fatalf("expected read in child tools, got %v", got)
	}
}

func TestSpawnDepthGuard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "x"},
			}},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	if err := agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore"}},
		MaxDepth: 1,
	}); err != nil {
		t.Fatal(err)
	}
	sp, ok := parent.Get("spawn")
	if !ok {
		t.Fatal("missing spawn")
	}
	ct, ok := sp.(tool.ContextualTool)
	if !ok {
		t.Fatal("spawn must be ContextualTool")
	}
	res := ct.RunCtx(agentspec.WithSpawnDepth(context.Background(), 1), tool.ExecutionMeta{}, map[string]any{
		"agent_type": "explore",
		"task":       "nested",
	})
	if res.Success {
		t.Fatal("expected depth exceeded")
	}
	if !strings.Contains(fmtString(res.Content), "depth exceeded") {
		t.Fatalf("content=%v", res.Content)
	}
}

func TestSpawnCancel(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	_ = agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore"}},
	})
	sp := mustSpawn(t, parent)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan tool.Result, 1)
	go func() {
		done <- sp.RunCtx(ctx, tool.ExecutionMeta{}, map[string]any{
			"agent_type": "explore",
			"task":       "slow",
		})
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	res := <-done
	if res.Success {
		t.Fatal("expected cancel failure")
	}
}

func TestSpawnMaxOutputTruncate(t *testing.T) {
	long := strings.Repeat("a", 200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": long},
			}},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	_ = agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children:  []agentspec.ChildSpec{{ID: "explore"}},
		MaxOutput: 50,
	})
	res := runSpawn(t, parent, map[string]any{"agent_type": "explore", "task": "x"})
	if !res.Success {
		t.Fatalf("%+v", res)
	}
	s, _ := res.Content.(string)
	if len(s) > 50 {
		t.Fatalf("len=%d content=%q", len(s), s)
	}
	if !strings.Contains(s, "truncated") {
		t.Fatalf("expected truncation marker, got %q", s)
	}
}

func TestSpawnProviderInherit(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "inherited"},
			}},
		})
	}))
	defer srv.Close()

	parent := emptyRegistry(t)
	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	_ = agentspec.RegisterSpawn(parent, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore"}}, // Provider nil → inherit
	})
	res := runSpawn(t, parent, map[string]any{"agent_type": "explore", "task": "hi"})
	if !res.Success || res.Content != "inherited" {
		t.Fatalf("%+v", res)
	}
	if hits.Load() == 0 {
		t.Fatal("parent provider not used")
	}
}

func TestSpawnAsNodeIntegration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		tools, _ := req["tools"].([]any)
		// First turn: if spawn is available, call it; else answer.
		hasSpawn := false
		for _, tl := range tools {
			tm, _ := tl.(map[string]any)
			fn, _ := tm["function"].(map[string]any)
			if fn["name"] == "spawn" {
				hasSpawn = true
			}
		}
		msgs, _ := req["messages"].([]any)
		alreadySpawned := false
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			if mm["role"] == "tool" {
				alreadySpawned = true
			}
		}
		if hasSpawn && !alreadySpawned {
			args, _ := json.Marshal(map[string]any{
				"agent_type": "explore",
				"task":       "scout",
			})
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []any{map[string]any{
					"finish_reason": "tool_calls",
					"message": map[string]any{
						"role": "assistant",
						"tool_calls": []any{map[string]any{
							"id":   "call_1",
							"type": "function",
							"function": map[string]any{
								"name":      "spawn",
								"arguments": string(args),
							},
						}},
					},
				}},
			})
			return
		}
		content := "parent done"
		if !hasSpawn {
			content = "child scout result"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": content},
			}},
		})
	}))
	defer srv.Close()

	prov := llm.NewProvider("gpt-4o", srv.URL, "k")
	reg := emptyRegistry(t)
	if err := agentspec.RegisterSpawn(reg, prov, agentspec.SpawnConfig{
		Children: []agentspec.ChildSpec{{ID: "explore", SystemPrompt: "scout"}},
	}); err != nil {
		t.Fatal(err)
	}
	inst := agentspec.New(agentspec.Spec{
		ID:       "coder",
		Provider: prov,
		Registry: reg,
		MaxIter:  5,
	})
	fn := inst.AsNode("input", "output")
	b := graph.NewBuilder()
	b.AddNode("coder", fn)
	b.SetEntry("coder")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Invoke(context.Background(), graph.State{"input": "use spawn"})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetString("output") == "" {
		t.Fatalf("empty output: %+v", out)
	}
}

// --- helpers ---

func emptyRegistry(t *testing.T) *tool.Registry {
	t.Helper()
	cfg := &config.Config{ProjectRoot: t.TempDir()}
	reg := tool.NewRegistry(cfg, console.Nop{}, nil, nil)
	// Start from full registry then clear via View of empty? View empty allowlist
	// returns all tools. Build a fresh registry by registering only what we need:
	// NewRegistry always fills builtins — fine for spawn registration tests.
	return reg
}

func runSpawn(t *testing.T, reg *tool.Registry, args map[string]any) tool.Result {
	t.Helper()
	sp, ok := reg.Get("spawn")
	if !ok {
		t.Fatal("spawn missing")
	}
	ct, ok := sp.(tool.ContextualTool)
	if !ok {
		t.Fatal("not ContextualTool")
	}
	return ct.RunCtx(context.Background(), tool.ExecutionMeta{}, args)
}

func fmtString(v any) string {
	s, _ := v.(string)
	return s
}

func mustSpawn(t *testing.T, reg *tool.Registry) tool.ContextualTool {
	t.Helper()
	sp, ok := reg.Get("spawn")
	if !ok {
		t.Fatal("spawn missing")
	}
	ct, ok := sp.(tool.ContextualTool)
	if !ok {
		t.Fatal("not ContextualTool")
	}
	return ct
}

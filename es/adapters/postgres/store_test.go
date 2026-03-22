package postgres

import (
	"strings"
	"testing"
)

func TestWithNotifyChannel(t *testing.T) {
	t.Parallel()

	config := NewStoreConfig(WithNotifyChannel("my_events"))

	if config.NotifyChannel != "my_events" {
		t.Errorf("NotifyChannel = %q, want %q", config.NotifyChannel, "my_events")
	}
}

func TestDefaultStoreConfig_NotifyChannelEmpty(t *testing.T) {
	t.Parallel()

	config := DefaultStoreConfig()

	if config.NotifyChannel != "" {
		t.Errorf("NotifyChannel = %q, want empty (disabled by default)", config.NotifyChannel)
	}
}

func TestBuildReadEventsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		scope        ReadEventsScope
		wantArgs     int
		wantContains []string
	}{
		{
			name:     "no scope filters",
			scope:    ReadEventsScope{},
			wantArgs: 2,
			wantContains: []string{
				"WHERE global_position > $1",
				"ORDER BY global_position ASC",
				"LIMIT $2",
			},
		},
		{
			name: "bounded context filter only",
			scope: ReadEventsScope{
				BoundedContexts: []string{"Billing"},
			},
			wantArgs: 3,
			wantContains: []string{
				"WHERE global_position > $1",
				"AND bounded_context = ANY($2)",
				"LIMIT $3",
			},
		},
		{
			name: "aggregate type filter only",
			scope: ReadEventsScope{
				AggregateTypes: []string{"User"},
			},
			wantArgs: 3,
			wantContains: []string{
				"WHERE global_position > $1",
				"AND aggregate_type = ANY($2)",
				"LIMIT $3",
			},
		},
		{
			name: "both filters",
			scope: ReadEventsScope{
				BoundedContexts: []string{"Billing"},
				AggregateTypes:  []string{"User", "Order"},
			},
			wantArgs: 4,
			wantContains: []string{
				"WHERE global_position > $1",
				"AND bounded_context = ANY($2)",
				"AND aggregate_type = ANY($3)",
				"LIMIT $4",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query, args := buildReadEventsQuery("events", 41, 25, tt.scope)
			normalized := strings.Join(strings.Fields(query), " ")

			for _, want := range tt.wantContains {
				if !strings.Contains(normalized, want) {
					t.Fatalf("query %q missing %q", normalized, want)
				}
			}

			if len(args) != tt.wantArgs {
				t.Fatalf("len(args) = %d, want %d", len(args), tt.wantArgs)
			}
			if args[0] != int64(41) {
				t.Fatalf("args[0] = %v, want 41", args[0])
			}
			if args[len(args)-1] != 25 {
				t.Fatalf("last arg = %v, want 25", args[len(args)-1])
			}
		})
	}
}

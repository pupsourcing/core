package postgres

import "testing"

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

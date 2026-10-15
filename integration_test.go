package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestReplayAMQP(t *testing.T) {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("set AMQP_URL to run the RabbitMQ integration test")
	}
	var conn *amqp.Connection
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("queue-recovery-%d", time.Now().UnixNano())
	source, target := prefix+"-dlq", prefix+"-jobs"
	for _, name := range []string{source, target} {
		if _, err := ch.QueueDeclare(name, false, true, true, false, nil); err != nil {
			ch.Close()
			conn.Close()
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = ch.QueueDelete(source, false, false, false)
		_, _ = ch.QueueDelete(target, false, false, false)
		_ = ch.Close()
		_ = conn.Close()
	})

	store, err := OpenIDStore(filepath.Join(t.TempDir(), "ids.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	publish := func() {
		t.Helper()
		err := ch.Publish("", source, false, false, amqp.Publishing{
			MessageId: "order-17", ContentType: "text/plain", Body: []byte("retry me"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	publish()
	if err := replay(context.Background(), ch, source, target, 1, store); err != nil {
		t.Fatal(err)
	}
	got, err := ch.QueueInspect(target)
	if err != nil || got.Messages != 1 {
		t.Fatalf("target messages=%d err=%v", got.Messages, err)
	}

	publish()
	if err := replay(context.Background(), ch, source, target, 1, store); err != nil {
		t.Fatal(err)
	}
	got, err = ch.QueueInspect(target)
	if err != nil || got.Messages != 1 {
		t.Fatalf("duplicate replay changed target count to %d, err=%v", got.Messages, err)
	}
	remaining, err := ch.QueueInspect(source)
	if err != nil || remaining.Messages != 0 {
		t.Fatalf("source messages=%d err=%v", remaining.Messages, err)
	}
}

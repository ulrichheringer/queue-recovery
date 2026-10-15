package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	mode := os.Args[1]
	flags := flag.NewFlagSet(mode, flag.ExitOnError)
	queue := flags.String("queue", "", "source dead-letter queue")
	target := flags.String("target", "", "target queue (replay only)")
	state := flags.String("state", "state.jsonl", "append-only file holding processed message IDs")
	limit := flags.Int("count", 100, "maximum messages to process")
	_ = flags.Parse(os.Args[2:])
	if *queue == "" || *limit < 1 || (mode != "inspect" && mode != "replay") || (mode == "replay" && *target == "") {
		usage()
		os.Exit(2)
	}
	url := os.Getenv("AMQP_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "set AMQP_URL to your RabbitMQ connection URL")
		os.Exit(2)
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		fatal(err)
	}
	defer ch.Close()

	if mode == "inspect" {
		q, err := ch.QueueInspect(*queue)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("queue=%s ready=%d consumers=%d\n", q.Name, q.Messages, q.Consumers)
		return
	}
	if *queue == *target {
		fatal(fmt.Errorf("source and target queues must differ"))
	}
	if _, err := ch.QueueDeclarePassive(*target, true, false, false, false, nil); err != nil {
		fatal(err)
	}
	store, err := OpenIDStore(*state)
	if err != nil {
		fatal(err)
	}
	if err := replay(context.Background(), ch, *queue, *target, *limit, store); err != nil {
		fatal(err)
	}
}

func replay(ctx context.Context, ch *amqp.Channel, source, target string, limit int, store *IDStore) error {
	if err := ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	processed := 0
	for processed < limit {
		d, ok, err := ch.Get(source, false)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if d.MessageId == "" {
			_ = d.Nack(false, true)
			return fmt.Errorf("message has no message_id; left in %q", source)
		}
		if store.Has(d.MessageId) {
			if err := d.Ack(false); err != nil {
				return err
			}
			processed++
			continue
		}
		msg := amqp.Publishing{
			Headers: d.Headers, ContentType: d.ContentType, ContentEncoding: d.ContentEncoding,
			DeliveryMode: d.DeliveryMode, Priority: d.Priority, CorrelationId: d.CorrelationId,
			ReplyTo: d.ReplyTo, Expiration: d.Expiration, MessageId: d.MessageId,
			Timestamp: d.Timestamp, Type: d.Type, UserId: d.UserId, AppId: d.AppId, Body: d.Body,
		}
		if err := ch.PublishWithContext(ctx, "", target, false, false, msg); err != nil {
			_ = d.Nack(false, true)
			return err
		}
		select {
		case confirmation, open := <-confirms:
			if !open || !confirmation.Ack {
				_ = d.Nack(false, true)
				return fmt.Errorf("broker did not confirm message %q", d.MessageId)
			}
		case <-ctx.Done():
			_ = d.Nack(false, true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			_ = d.Nack(false, true)
			return fmt.Errorf("timed out waiting for publish confirmation for %q", d.MessageId)
		}
		if err := store.Mark(d.MessageId); err != nil {
			_ = d.Nack(false, true)
			return err
		}
		if err := d.Ack(false); err != nil {
			return err
		}
		processed++
		fmt.Printf("replayed message_id=%s\n", d.MessageId)
	}
	fmt.Printf("processed=%d\n", processed)
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: queue-recovery inspect --queue DLQ | replay --queue DLQ --target QUEUE [--state state.json] [--count 100]")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

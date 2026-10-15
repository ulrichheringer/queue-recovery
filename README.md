# queue-recovery

Inspect a RabbitMQ queue and move dead-lettered messages back to a target queue. Replay uses manual acknowledgements, publisher confirms, and a local message-ID ledger.

Install with go install github.com/ulrichheringer/queue-recovery@latest. Set AMQP_URL, then run queue-recovery inspect --queue failed-jobs or queue-recovery replay --queue failed-jobs --target jobs. Use --state to choose the ledger file. For a local broker, run docker compose up -d; the integration test runs with AMQP_URL=amqp://lab:local-lab@localhost:5672/.

The local ledger prevents repeat replays after a confirmed publish. Keep its file durable and shared by one recovery process; RabbitMQ and a local file cannot provide atomic exactly-once delivery across a process crash.

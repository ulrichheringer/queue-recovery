# queue-recovery

A command-line tool for checking a RabbitMQ queue and replaying its messages to another queue. It records message IDs locally so messages it has already replayed can be skipped.

Install with go install github.com/ulrichheringer/queue-recovery@latest. Set AMQP_URL, then run queue-recovery inspect --queue failed-jobs or queue-recovery replay --queue failed-jobs --target jobs.

For a local broker, run docker compose up -d. To run the integration test, set AMQP_URL to amqp://lab:local-lab@localhost:5672/ and run go test ./....

Keep the ID ledger on persistent storage and use one recovery process. A process crash between publishing and recording an ID can still result in a duplicate.

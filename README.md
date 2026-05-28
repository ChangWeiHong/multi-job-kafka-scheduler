# Multi-Job Kafka Scheduler

A local NestJS learning project that stores jobs in Postgres, publishes due work to Kafka, runs fake typed workers, retries failures, and records execution history.

## Local Run

```bash
cp .env.example .env
npm install
docker-compose up -d
npm run prisma:deploy
npm run start:dev
```

If your Docker installation has the newer Compose plugin, `docker compose up -d` works too.
Postgres is exposed on host port `15432` to avoid clashing with any existing local Postgres.

In another terminal:

```bash
npm run smoke
```

## API

```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "welcome email",
    "type": "email",
    "payload": { "to": "student@example.com" },
    "scheduleType": "immediate",
    "maxAttempts": 3
  }'

curl http://localhost:3000/jobs
curl http://localhost:3000/jobs/JOB_ID
curl http://localhost:3000/jobs/JOB_ID/executions
```

Read [doc.html](./doc.html) for the learning plan and system design.

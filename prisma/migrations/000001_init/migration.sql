CREATE TYPE "JobType" AS ENUM ('email', 'billing', 'report', 'export');
CREATE TYPE "JobStatus" AS ENUM ('scheduled', 'queued', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE "ScheduleType" AS ENUM ('immediate', 'delayed', 'recurring');
CREATE TYPE "ExecutionStatus" AS ENUM ('queued', 'running', 'succeeded', 'failed');

CREATE TABLE "jobs" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "name" TEXT NOT NULL,
  "type" "JobType" NOT NULL,
  "payload" JSONB NOT NULL,
  "status" "JobStatus" NOT NULL DEFAULT 'scheduled',
  "schedule_type" "ScheduleType" NOT NULL,
  "next_run_at" TIMESTAMPTZ(6) NOT NULL,
  "recurrence_interval_seconds" INTEGER,
  "max_attempts" INTEGER NOT NULL DEFAULT 3,
  "attempts_made" INTEGER NOT NULL DEFAULT 0,
  "locked_until" TIMESTAMPTZ(6),
  "last_error" TEXT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "jobs_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "job_executions" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "job_id" UUID NOT NULL,
  "status" "ExecutionStatus" NOT NULL DEFAULT 'queued',
  "attempt" INTEGER NOT NULL,
  "topic" TEXT NOT NULL,
  "message_key" TEXT NOT NULL,
  "started_at" TIMESTAMPTZ(6),
  "completed_at" TIMESTAMPTZ(6),
  "error_message" TEXT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "job_executions_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "jobs_status_next_run_at_idx" ON "jobs"("status", "next_run_at");
CREATE INDEX "job_executions_job_id_attempt_idx" ON "job_executions"("job_id", "attempt");

ALTER TABLE "job_executions"
  ADD CONSTRAINT "job_executions_job_id_fkey"
  FOREIGN KEY ("job_id") REFERENCES "jobs"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

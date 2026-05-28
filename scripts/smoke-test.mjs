import 'dotenv/config';
import { Kafka, Partitioners } from 'kafkajs';

const baseUrl = process.env.BASE_URL ?? 'http://localhost:3000';
const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim());

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function request(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers ?? {}),
    },
  });

  const text = await response.text();
  const body = text ? JSON.parse(text) : null;

  if (!response.ok) {
    throw new Error(`${options.method ?? 'GET'} ${path} failed: ${response.status} ${text}`);
  }

  return body;
}

async function pollJob(jobId) {
  const deadline = Date.now() + 60000;

  while (Date.now() < deadline) {
    const job = await request(`/jobs/${jobId}`);
    if (job.status === 'completed' || job.status === 'failed') {
      return job;
    }
    await sleep(2000);
  }

  throw new Error(`Job ${jobId} did not finish within 60 seconds`);
}

async function createJob(body) {
  return request('/jobs', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

async function publishDuplicate(job, execution) {
  const kafka = new Kafka({
    clientId: 'multi-job-smoke-test',
    brokers: kafkaBrokers,
  });
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });
  await producer.connect();
  try {
    await producer.send({
      topic: execution.topic,
      messages: [
        {
          key: execution.messageKey,
          value: JSON.stringify({
            jobId: job.id,
            executionId: execution.id,
            type: job.type,
            attempt: execution.attempt,
            payload: job.payload,
          }),
        },
      ],
    });
  } finally {
    await producer.disconnect();
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

const health = await request('/health');
assert(health.ok, 'Health check failed');

const expectedTopics = {
  email: 'jobs.email',
  billing: 'jobs.billing',
  report: 'jobs.report',
  export: 'jobs.export',
};

const immediateJobs = await Promise.all(
  Object.keys(expectedTopics).map((type) =>
    createJob({
      name: `smoke-${type}-${Date.now()}`,
      type,
      payload: {
        to: 'student@example.com',
        kind: type,
      },
      scheduleType: 'immediate',
      maxAttempts: 3,
    }),
  ),
);

const completedJobs = [];
for (const job of immediateJobs) {
  const finishedJob = await pollJob(job.id);
  const executions = await request(`/jobs/${job.id}/executions`);
  assert(finishedJob.status === 'completed', `Expected completed ${job.type} job, got ${finishedJob.status}`);
  assert(executions.length === 1, `Expected one execution for ${job.type}, got ${executions.length}`);
  assert(executions[0].status === 'succeeded', `Expected succeeded execution for ${job.type}`);
  assert(executions[0].topic === expectedTopics[job.type], `Unexpected topic for ${job.type}`);
  completedJobs.push({ job: finishedJob, executions });
}

const duplicateTarget = completedJobs.find(({ job }) => job.type === 'email');
await publishDuplicate(duplicateTarget.job, duplicateTarget.executions[0]);
await sleep(3000);
const executionsAfterDuplicate = await request(`/jobs/${duplicateTarget.job.id}/executions`);
assert(executionsAfterDuplicate.length === 1, 'Duplicate message should not create another execution');
assert(executionsAfterDuplicate[0].status === 'succeeded', 'Duplicate message should not change terminal status');

const delayedRunAt = new Date(Date.now() + 8000).toISOString();
const delayedJob = await createJob({
  name: `smoke-delayed-${Date.now()}`,
  type: 'report',
  payload: { report: 'delayed' },
  scheduleType: 'delayed',
  runAt: delayedRunAt,
  maxAttempts: 3,
});
await sleep(3000);
const earlyDelayedExecutions = await request(`/jobs/${delayedJob.id}/executions`);
assert(earlyDelayedExecutions.length === 0, 'Delayed job published before runAt');
const completedDelayedJob = await pollJob(delayedJob.id);
assert(completedDelayedJob.status === 'completed', `Expected delayed job completed, got ${completedDelayedJob.status}`);

const failingJob = await createJob({
  name: `smoke-failing-${Date.now()}`,
  type: 'export',
  payload: { forceFail: true },
  scheduleType: 'immediate',
  maxAttempts: 2,
});
const failedJob = await pollJob(failingJob.id);
const failedExecutions = await request(`/jobs/${failingJob.id}/executions`);
assert(failedJob.status === 'failed', `Expected failed job, got ${failedJob.status}`);
assert(failedExecutions.length === 2, `Expected two failed attempts, got ${failedExecutions.length}`);
assert(failedExecutions.every((execution) => execution.status === 'failed'), 'Expected all retry executions to fail');
assert(failedExecutions.map((execution) => execution.attempt).join(',') === '1,2', 'Expected retry attempts 1,2');

const recurringJob = await createJob({
  name: `smoke-recurring-${Date.now()}`,
  type: 'billing',
  payload: { accountId: 'acct_smoke' },
  scheduleType: 'recurring',
  recurrenceIntervalSeconds: 60,
  maxAttempts: 3,
});
const recurringDeadline = Date.now() + 60000;
let rescheduledRecurringJob;
while (Date.now() < recurringDeadline) {
  const current = await request(`/jobs/${recurringJob.id}`);
  const executions = await request(`/jobs/${recurringJob.id}/executions`);
  if (current.status === 'scheduled' && executions.some((execution) => execution.status === 'succeeded')) {
    rescheduledRecurringJob = current;
    break;
  }
  await sleep(2000);
}
assert(rescheduledRecurringJob, 'Recurring job did not reschedule after success');
assert(new Date(rescheduledRecurringJob.nextRunAt).getTime() > Date.now(), 'Recurring nextRunAt should be in the future');

const listedJobs = await request('/jobs');
assert(Array.isArray(listedJobs) && listedJobs.length > 0, 'GET /jobs should list recent jobs');

console.log(
  JSON.stringify(
    {
      ok: true,
      immediateTypes: completedJobs.map(({ job }) => job.type),
      delayedJobId: delayedJob.id,
      failedJobId: failingJob.id,
      recurringJobId: recurringJob.id,
      duplicateGuardJobId: duplicateTarget.job.id,
    },
    null,
    2,
  ),
);

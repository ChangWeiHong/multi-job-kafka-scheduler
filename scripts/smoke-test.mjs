const baseUrl = process.env.BASE_URL ?? 'http://localhost:3000';

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

const health = await request('/health');
if (!health.ok) {
  throw new Error('Health check failed');
}

const job = await request('/jobs', {
  method: 'POST',
  body: JSON.stringify({
    name: `smoke-email-${Date.now()}`,
    type: 'email',
    payload: {
      to: 'student@example.com',
      template: 'welcome',
    },
    scheduleType: 'immediate',
    maxAttempts: 3,
  }),
});

const finishedJob = await pollJob(job.id);
const executions = await request(`/jobs/${job.id}/executions`);

if (finishedJob.status !== 'completed') {
  throw new Error(`Expected completed job, got ${finishedJob.status}`);
}

if (executions.length !== 1 || executions[0].status !== 'succeeded') {
  throw new Error(`Expected one succeeded execution, got ${JSON.stringify(executions)}`);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      jobId: job.id,
      status: finishedJob.status,
      executionStatus: executions[0].status,
      topic: executions[0].topic,
    },
    null,
    2,
  ),
);

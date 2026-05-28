import { JobType, Prisma } from '@prisma/client';

export type JobMessage = {
  jobId: string;
  executionId: string;
  type: JobType;
  attempt: number;
  payload: Prisma.JsonValue;
};

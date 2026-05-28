import { JobType } from '@prisma/client';

export const JOB_TYPES: JobType[] = [
  JobType.email,
  JobType.billing,
  JobType.report,
  JobType.export,
];

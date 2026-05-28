import { JobType } from '@prisma/client';

export const JOB_TOPIC_BY_TYPE: Record<JobType, string> = {
  [JobType.email]: 'jobs.email',
  [JobType.billing]: 'jobs.billing',
  [JobType.report]: 'jobs.report',
  [JobType.export]: 'jobs.export',
};

export function topicForType(type: JobType): string {
  return JOB_TOPIC_BY_TYPE[type];
}

export function consumerGroupForType(type: JobType): string {
  return `${type}-workers`;
}

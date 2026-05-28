import { JobType } from '@prisma/client';

import { consumerGroupForType, topicForType } from '../src/kafka/topic-map';

describe('topic map', () => {
  it.each([
    [JobType.email, 'jobs.email', 'email-workers'],
    [JobType.billing, 'jobs.billing', 'billing-workers'],
    [JobType.report, 'jobs.report', 'report-workers'],
    [JobType.export, 'jobs.export', 'export-workers'],
  ])('maps %s jobs', (type, topic, group) => {
    expect(topicForType(type)).toBe(topic);
    expect(consumerGroupForType(type)).toBe(group);
  });
});

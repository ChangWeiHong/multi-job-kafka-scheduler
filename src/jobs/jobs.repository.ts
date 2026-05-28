import { Injectable } from '@nestjs/common';
import { JobStatus, JobType, Prisma, ScheduleType } from '@prisma/client';

import { PrismaService } from '../prisma/prisma.service';

type CreateJobInput = {
  name: string;
  type: JobType;
  payload: Prisma.InputJsonValue;
  scheduleType: ScheduleType;
  nextRunAt: Date;
  recurrenceIntervalSeconds?: number;
  maxAttempts: number;
};

@Injectable()
export class JobsRepository {
  constructor(private readonly prisma: PrismaService) {}

  createJob(input: CreateJobInput) {
    return this.prisma.job.create({
      data: {
        name: input.name,
        type: input.type,
        payload: input.payload,
        status: JobStatus.scheduled,
        scheduleType: input.scheduleType,
        nextRunAt: input.nextRunAt,
        recurrenceIntervalSeconds: input.recurrenceIntervalSeconds,
        maxAttempts: input.maxAttempts,
      },
    });
  }

  listJobs() {
    return this.prisma.job.findMany({
      orderBy: { createdAt: 'desc' },
      take: 50,
      include: {
        executions: {
          orderBy: { createdAt: 'desc' },
          take: 1,
        },
      },
    });
  }

  findJobById(id: string) {
    return this.prisma.job.findUnique({
      where: { id },
      include: {
        executions: {
          orderBy: { createdAt: 'desc' },
          take: 1,
        },
      },
    });
  }

  findExecutionsByJobId(jobId: string) {
    return this.prisma.jobExecution.findMany({
      where: { jobId },
      orderBy: [{ attempt: 'asc' }, { createdAt: 'asc' }],
    });
  }
}

import { Injectable, Logger } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { ExecutionStatus, Job, JobExecution, JobStatus } from '@prisma/client';
import { randomUUID } from 'node:crypto';

import { lockUntil, retryDelaySeconds, secondsFromNow } from '../common/time';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { topicForType } from '../kafka/topic-map';
import { PrismaService } from '../prisma/prisma.service';

type ClaimedJob = {
  job: Job;
  execution: JobExecution;
};

@Injectable()
export class SchedulerService {
  private readonly logger = new Logger(SchedulerService.name);
  private running = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly producer: KafkaProducerService,
  ) {}

  @Interval(5000)
  async tick() {
    if (this.running) {
      return;
    }

    this.running = true;
    try {
      await this.recoverExpiredJobs();
      const claimedJobs = await this.claimDueJobs();
      await Promise.all(claimedJobs.map((claimedJob) => this.publishClaimedJob(claimedJob)));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown scheduler error';
      this.logger.error(message);
    } finally {
      this.running = false;
    }
  }

  private async claimDueJobs(batchSize = 25): Promise<ClaimedJob[]> {
    return this.prisma.$transaction(async (tx) => {
      const rows = await tx.$queryRaw<Array<{ id: string }>>`
        SELECT id
        FROM jobs
        WHERE status = 'scheduled'
          AND next_run_at <= now()
          AND (locked_until IS NULL OR locked_until < now())
        ORDER BY next_run_at ASC
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      `;

      const claimedJobs: ClaimedJob[] = [];

      for (const row of rows) {
        const job = await tx.job.findUniqueOrThrow({ where: { id: row.id } });
        const executionId = randomUUID();
        const attempt = job.attemptsMade + 1;
        const topic = topicForType(job.type);

        const execution = await tx.jobExecution.create({
          data: {
            id: executionId,
            jobId: job.id,
            status: ExecutionStatus.queued,
            attempt,
            topic,
            messageKey: executionId,
          },
        });

        const updatedJob = await tx.job.update({
          where: { id: job.id },
          data: {
            status: JobStatus.queued,
            attemptsMade: attempt,
            lockedUntil: lockUntil(),
            lastError: null,
          },
        });

        claimedJobs.push({ job: updatedJob, execution });
      }

      return claimedJobs;
    });
  }

  private async publishClaimedJob({ job, execution }: ClaimedJob) {
    try {
      await this.producer.publish(execution.topic, execution.messageKey, {
        jobId: job.id,
        executionId: execution.id,
        type: job.type,
        attempt: execution.attempt,
        payload: job.payload,
      });
      this.logger.log(`Published execution ${execution.id} to ${execution.topic}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown Kafka publish error';
      this.logger.error(`Failed to publish execution ${execution.id}: ${message}`);
      await this.markPublishFailure(execution.id, message);
    }
  }

  private async markPublishFailure(executionId: string, errorMessage: string) {
    await this.prisma.$transaction(async (tx) => {
      const execution = await tx.jobExecution.findUniqueOrThrow({
        where: { id: executionId },
        include: { job: true },
      });
      const attemptsRemain = execution.job.attemptsMade < execution.job.maxAttempts;

      await tx.jobExecution.update({
        where: { id: execution.id },
        data: {
          status: ExecutionStatus.failed,
          completedAt: new Date(),
          errorMessage,
        },
      });

      await tx.job.update({
        where: { id: execution.jobId },
        data: attemptsRemain
          ? {
              status: JobStatus.scheduled,
              nextRunAt: secondsFromNow(retryDelaySeconds(execution.attempt)),
              lockedUntil: null,
              lastError: errorMessage,
            }
          : {
              status: JobStatus.failed,
              lockedUntil: null,
              lastError: errorMessage,
            },
      });
    });
  }

  private async recoverExpiredJobs() {
    const expiredJobs = await this.prisma.job.findMany({
      where: {
        status: { in: [JobStatus.queued, JobStatus.running] },
        lockedUntil: { lt: new Date() },
      },
      take: 100,
    });

    for (const job of expiredJobs) {
      const errorMessage = 'Execution lock expired before completion';
      const attemptsRemain = job.attemptsMade < job.maxAttempts;

      await this.prisma.$transaction([
        this.prisma.jobExecution.updateMany({
          where: {
            jobId: job.id,
            status: { in: [ExecutionStatus.queued, ExecutionStatus.running] },
          },
          data: {
            status: ExecutionStatus.failed,
            completedAt: new Date(),
            errorMessage,
          },
        }),
        this.prisma.job.update({
          where: { id: job.id },
          data: attemptsRemain
            ? {
                status: JobStatus.scheduled,
                nextRunAt: secondsFromNow(retryDelaySeconds(job.attemptsMade)),
                lockedUntil: null,
                lastError: errorMessage,
              }
            : {
                status: JobStatus.failed,
                lockedUntil: null,
                lastError: errorMessage,
              },
        }),
      ]);
    }
  }
}

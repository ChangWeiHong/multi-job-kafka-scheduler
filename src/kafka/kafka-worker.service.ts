import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ExecutionStatus, Job, JobStatus, JobType, ScheduleType } from '@prisma/client';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';

import { retryDelaySeconds, secondsFromNow, lockUntil } from '../common/time';
import { BillingHandler } from '../handlers/billing.handler';
import { EmailHandler } from '../handlers/email.handler';
import { ExportHandler } from '../handlers/export.handler';
import { ReportHandler } from '../handlers/report.handler';
import { PrismaService } from '../prisma/prisma.service';
import { JobMessage } from './job-message';
import { consumerGroupForType, JOB_TOPIC_BY_TYPE, topicForType } from './topic-map';

@Injectable()
export class KafkaWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaWorkerService.name);
  private readonly enabled = process.env.KAFKA_ENABLED !== 'false';
  private readonly kafka = new Kafka({
    clientId: `${process.env.KAFKA_CLIENT_ID ?? 'multi-job-kafka-scheduler'}-workers`,
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()),
    retry: { retries: 8 },
  });
  private readonly consumers: Consumer[] = [];

  constructor(
    private readonly prisma: PrismaService,
    private readonly emailHandler: EmailHandler,
    private readonly billingHandler: BillingHandler,
    private readonly reportHandler: ReportHandler,
    private readonly exportHandler: ExportHandler,
  ) {}

  async onModuleInit() {
    if (!this.enabled) {
      this.logger.warn('Kafka workers are disabled with KAFKA_ENABLED=false');
      return;
    }

    for (const type of Object.keys(JOB_TOPIC_BY_TYPE) as JobType[]) {
      await this.startConsumer(type);
    }
  }

  async onModuleDestroy() {
    await Promise.all(this.consumers.map((consumer) => consumer.disconnect()));
  }

  private async startConsumer(type: JobType) {
    const topic = topicForType(type);
    const consumer = this.kafka.consumer({ groupId: consumerGroupForType(type) });

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    await consumer.run({
      eachMessage: (payload) => this.handleKafkaMessage(payload),
    });

    this.consumers.push(consumer);
    this.logger.log(`Worker connected for ${type} on ${topic}`);
  }

  private async handleKafkaMessage(payload: EachMessagePayload) {
    const value = payload.message.value?.toString();
    if (!value) {
      return;
    }

    const message = JSON.parse(value) as JobMessage;
    await this.processMessage(message);
  }

  private async processMessage(message: JobMessage) {
    const execution = await this.prisma.jobExecution.findUnique({
      where: { id: message.executionId },
      include: { job: true },
    });

    if (!execution) {
      this.logger.warn(`Execution ${message.executionId} was not found`);
      return;
    }

    if (execution.status === ExecutionStatus.succeeded || execution.status === ExecutionStatus.failed) {
      this.logger.log(`Execution ${message.executionId} already finished; skipping duplicate message`);
      return;
    }

    await this.prisma.$transaction([
      this.prisma.jobExecution.update({
        where: { id: execution.id },
        data: {
          status: ExecutionStatus.running,
          startedAt: new Date(),
          errorMessage: null,
        },
      }),
      this.prisma.job.update({
        where: { id: execution.jobId },
        data: {
          status: JobStatus.running,
          lockedUntil: lockUntil(),
        },
      }),
    ]);

    try {
      await this.runHandler(execution.job);
      await this.markExecutionSucceeded(execution.id);
    } catch (error) {
      await this.markExecutionFailed(execution.id, error);
    }
  }

  private runHandler(job: Job) {
    switch (job.type) {
      case JobType.email:
        return this.emailHandler.handle(job);
      case JobType.billing:
        return this.billingHandler.handle(job);
      case JobType.report:
        return this.reportHandler.handle(job);
      case JobType.export:
        return this.exportHandler.handle(job);
    }
  }

  private async markExecutionSucceeded(executionId: string) {
    await this.prisma.$transaction(async (tx) => {
      const execution = await tx.jobExecution.findUniqueOrThrow({
        where: { id: executionId },
        include: { job: true },
      });

      await tx.jobExecution.update({
        where: { id: execution.id },
        data: {
          status: ExecutionStatus.succeeded,
          completedAt: new Date(),
          errorMessage: null,
        },
      });

      if (execution.job.scheduleType === ScheduleType.recurring && execution.job.recurrenceIntervalSeconds) {
        await tx.job.update({
          where: { id: execution.jobId },
          data: {
            status: JobStatus.scheduled,
            attemptsMade: 0,
            nextRunAt: secondsFromNow(execution.job.recurrenceIntervalSeconds),
            lockedUntil: null,
            lastError: null,
          },
        });
        return;
      }

      await tx.job.update({
        where: { id: execution.jobId },
        data: {
          status: JobStatus.completed,
          lockedUntil: null,
          lastError: null,
        },
      });
    });
  }

  private async markExecutionFailed(executionId: string, error: unknown) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown job handler error';

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
}

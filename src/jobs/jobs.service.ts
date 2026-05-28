import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { Prisma, ScheduleType } from '@prisma/client';

import { CreateJobDto } from './dto/create-job.dto';
import { JobsRepository } from './jobs.repository';

@Injectable()
export class JobsService {
  constructor(private readonly jobsRepository: JobsRepository) {}

  createJob(dto: CreateJobDto) {
    const nextRunAt = this.resolveNextRunAt(dto);

    return this.jobsRepository.createJob({
      name: dto.name,
      type: dto.type,
      payload: dto.payload as Prisma.InputJsonObject,
      scheduleType: dto.scheduleType,
      nextRunAt,
      recurrenceIntervalSeconds: dto.recurrenceIntervalSeconds,
      maxAttempts: dto.maxAttempts ?? 3,
    });
  }

  listJobs() {
    return this.jobsRepository.listJobs();
  }

  async getJob(id: string) {
    const job = await this.jobsRepository.findJobById(id);
    if (!job) {
      throw new NotFoundException(`Job ${id} was not found`);
    }
    return job;
  }

  async getExecutions(jobId: string) {
    await this.getJob(jobId);
    return this.jobsRepository.findExecutionsByJobId(jobId);
  }

  private resolveNextRunAt(dto: CreateJobDto): Date {
    if (dto.scheduleType === ScheduleType.immediate) {
      return new Date();
    }

    if (dto.scheduleType === ScheduleType.delayed) {
      if (!dto.runAt) {
        throw new BadRequestException('runAt is required for delayed jobs');
      }
      return new Date(dto.runAt);
    }

    if (!dto.recurrenceIntervalSeconds) {
      throw new BadRequestException('recurrenceIntervalSeconds is required for recurring jobs');
    }

    return dto.runAt ? new Date(dto.runAt) : new Date();
  }
}

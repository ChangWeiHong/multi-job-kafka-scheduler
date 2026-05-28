import { Injectable, Logger } from '@nestjs/common';
import { Job } from '@prisma/client';

import { asPayloadObject, failIfRequested } from './handler-utils';

@Injectable()
export class ReportHandler {
  private readonly logger = new Logger(ReportHandler.name);

  async handle(job: Job) {
    const payload = asPayloadObject(job.payload);
    failIfRequested(payload);
    this.logger.log(`Pretend report generated for job ${job.id}`);
  }
}

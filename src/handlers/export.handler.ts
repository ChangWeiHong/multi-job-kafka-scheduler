import { Injectable, Logger } from '@nestjs/common';
import { Job } from '@prisma/client';

import { asPayloadObject, failIfRequested } from './handler-utils';

@Injectable()
export class ExportHandler {
  private readonly logger = new Logger(ExportHandler.name);

  async handle(job: Job) {
    const payload = asPayloadObject(job.payload);
    failIfRequested(payload);
    this.logger.log(`Pretend export produced for job ${job.id}`);
  }
}

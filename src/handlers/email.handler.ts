import { Injectable, Logger } from '@nestjs/common';
import { Job } from '@prisma/client';

import { asPayloadObject, failIfRequested } from './handler-utils';

@Injectable()
export class EmailHandler {
  private readonly logger = new Logger(EmailHandler.name);

  async handle(job: Job) {
    const payload = asPayloadObject(job.payload);
    failIfRequested(payload);
    this.logger.log(`Pretend email sent for job ${job.id} to ${String(payload.to ?? 'unknown')}`);
  }
}

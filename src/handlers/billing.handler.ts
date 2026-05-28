import { Injectable, Logger } from '@nestjs/common';
import { Job } from '@prisma/client';

import { asPayloadObject, failIfRequested, sleep } from './handler-utils';

@Injectable()
export class BillingHandler {
  private readonly logger = new Logger(BillingHandler.name);

  async handle(job: Job) {
    const payload = asPayloadObject(job.payload);
    failIfRequested(payload);
    await sleep(1000);
    this.logger.log(`Pretend billing task completed for job ${job.id}`);
  }
}

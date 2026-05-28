import { Module } from '@nestjs/common';

import { JobsController } from './jobs.controller';
import { JobsRepository } from './jobs.repository';
import { JobsService } from './jobs.service';

@Module({
  controllers: [JobsController],
  providers: [JobsRepository, JobsService],
})
export class JobsModule {}

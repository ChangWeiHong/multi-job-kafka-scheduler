import { Module } from '@nestjs/common';

import { AppKafkaModule } from '../kafka/kafka.module';
import { SchedulerService } from './scheduler.service';

@Module({
  imports: [AppKafkaModule],
  providers: [SchedulerService],
})
export class JobSchedulerModule {}

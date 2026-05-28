import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { AppController } from './app.controller';
import { HandlersModule } from './handlers/handlers.module';
import { JobsModule } from './jobs/jobs.module';
import { AppKafkaModule } from './kafka/kafka.module';
import { PrismaModule } from './prisma/prisma.module';
import { JobSchedulerModule } from './scheduler/scheduler.module';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    PrismaModule,
    JobsModule,
    HandlersModule,
    AppKafkaModule,
    JobSchedulerModule,
  ],
  controllers: [AppController],
})
export class AppModule {}

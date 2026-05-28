import { Module } from '@nestjs/common';

import { HandlersModule } from '../handlers/handlers.module';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaWorkerService } from './kafka-worker.service';

@Module({
  imports: [HandlersModule],
  providers: [KafkaProducerService, KafkaWorkerService],
  exports: [KafkaProducerService],
})
export class AppKafkaModule {}

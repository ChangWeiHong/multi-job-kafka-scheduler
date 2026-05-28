import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

import { JobMessage } from './job-message';
import { JOB_TOPIC_BY_TYPE } from './topic-map';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly enabled = process.env.KAFKA_ENABLED !== 'false';
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'multi-job-kafka-scheduler',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()),
    retry: { retries: 8 },
  });
  private producer?: Producer;

  async onModuleInit() {
    if (!this.enabled) {
      this.logger.warn('Kafka is disabled with KAFKA_ENABLED=false');
      return;
    }

    await this.connectWithRetry();
  }

  async onModuleDestroy() {
    await this.producer?.disconnect();
  }

  async publish(topic: string, key: string, message: JobMessage) {
    if (!this.enabled) {
      this.logger.log(`Kafka disabled, would publish ${message.executionId} to ${topic}`);
      return;
    }

    if (!this.producer) {
      throw new Error('Kafka producer is not connected');
    }

    await this.producer.send({
      topic,
      messages: [
        {
          key,
          value: JSON.stringify(message),
        },
      ],
    });
  }

  private async connectWithRetry() {
    let lastError: unknown;

    for (let attempt = 1; attempt <= 12; attempt += 1) {
      try {
        const admin = this.kafka.admin();
        await admin.connect();
        await admin.createTopics({
          waitForLeaders: true,
          topics: Object.values(JOB_TOPIC_BY_TYPE).map((topic) => ({
            topic,
            numPartitions: 1,
            replicationFactor: 1,
          })),
        });
        await admin.disconnect();

        this.producer = this.kafka.producer();
        await this.producer.connect();
        this.logger.log('Kafka producer connected');
        return;
      } catch (error) {
        lastError = error;
        this.logger.warn(`Kafka producer connection attempt ${attempt} failed`);
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }

    throw lastError instanceof Error ? lastError : new Error('Kafka producer failed to connect');
  }
}

import 'reflect-metadata';
import 'dotenv/config';

import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  Logger.log(`Multi-job Kafka scheduler listening on http://localhost:${port}`, 'Bootstrap');
}

void bootstrap();

import { Type } from 'class-transformer';
import {
  IsEnum,
  IsInt,
  IsISO8601,
  IsObject,
  IsOptional,
  IsString,
  Max,
  Min,
  MinLength,
} from 'class-validator';
import { JobType, ScheduleType } from '@prisma/client';

export class CreateJobDto {
  @IsString()
  @MinLength(1)
  name!: string;

  @IsEnum(JobType)
  type!: JobType;

  @IsObject()
  payload!: Record<string, unknown>;

  @IsEnum(ScheduleType)
  scheduleType!: ScheduleType;

  @IsOptional()
  @IsISO8601()
  runAt?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  recurrenceIntervalSeconds?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(10)
  maxAttempts?: number;
}

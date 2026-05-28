import { Body, Controller, Get, Param, ParseUUIDPipe, Post } from '@nestjs/common';

import { CreateJobDto } from './dto/create-job.dto';
import { JobsService } from './jobs.service';

@Controller('jobs')
export class JobsController {
  constructor(private readonly jobsService: JobsService) {}

  @Post()
  createJob(@Body() dto: CreateJobDto) {
    return this.jobsService.createJob(dto);
  }

  @Get()
  listJobs() {
    return this.jobsService.listJobs();
  }

  @Get(':id/executions')
  getExecutions(@Param('id', ParseUUIDPipe) id: string) {
    return this.jobsService.getExecutions(id);
  }

  @Get(':id')
  getJob(@Param('id', ParseUUIDPipe) id: string) {
    return this.jobsService.getJob(id);
  }
}

import { Module } from '@nestjs/common';

import { BillingHandler } from './billing.handler';
import { EmailHandler } from './email.handler';
import { ExportHandler } from './export.handler';
import { ReportHandler } from './report.handler';

@Module({
  providers: [EmailHandler, BillingHandler, ReportHandler, ExportHandler],
  exports: [EmailHandler, BillingHandler, ReportHandler, ExportHandler],
})
export class HandlersModule {}

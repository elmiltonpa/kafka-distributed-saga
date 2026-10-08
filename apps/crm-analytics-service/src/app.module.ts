import { Module } from '@nestjs/common';
import { AnalyticsModule } from './analytics/analytics.module';

@Module({
  imports: [AnalyticsModule],
  controllers: [],
  providers: [],
})
export class AppModule {}

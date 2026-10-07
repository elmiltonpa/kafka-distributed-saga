import { Module } from '@nestjs/common';
import { ActivationsModule } from './activations/activations.module';

@Module({
  imports: [ActivationsModule],
})
export class AppModule {}

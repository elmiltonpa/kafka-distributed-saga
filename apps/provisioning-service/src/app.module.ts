import { Module } from '@nestjs/common';
import { ProvisioningModule } from './provisioning/provisioning.module';

@Module({
  imports: [ProvisioningModule],
})
export class AppModule {}

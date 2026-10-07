export class CreateActivationDto {
  customerId!: string;
  planId!: string;
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}

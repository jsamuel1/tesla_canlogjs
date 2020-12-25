import * as cdk from '@aws-cdk/core';
import { Vpc }  from '@aws-cdk/aws-ec2';

export class CdkEnvironmentStack extends cdk.Stack {
    vpc: Vpc;
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.vpc = new Vpc(this, 'vpc', { cidr: "10.1.0.0/16" })
    
  }
}

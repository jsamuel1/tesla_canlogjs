#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkStack } from '../lib/cdk-stack';
import { CdkGrafanaStack } from '../lib/cdk-grafana-stack';
import { CdkEnvironmentStack } from '../lib/cdk-environment-stack';

const app = new cdk.App();

const envEU = { region: 'eu-west-1', account: process.env.CDK_DEFAULT_ACCOUNT };
const envStack = new CdkEnvironmentStack(app, 'CdkEnvironmentStack', { env: envEU })
new CdkStack(app, 'CdkStack', { env: envEU, vpc: envStack.vpc });
new CdkGrafanaStack(app, 'CdkGrafanaStack', { env: envEU, vpc: envStack.vpc })
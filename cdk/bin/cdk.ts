#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkStack } from '../lib/cdk-stack';

const app = new cdk.App();

const envEU = { region: 'eu-west-1' };
new CdkStack(app, 'CdkStack', { env: envEU });

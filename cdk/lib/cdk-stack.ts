import * as cdk from '@aws-cdk/core';
import { CfnDatabase, CfnTable } from '@aws-cdk/aws-timestream';
import { PythonFunction } from '@aws-cdk/aws-lambda-python';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iam from '@aws-cdk/aws-iam';
import { Duration } from '@aws-cdk/core';
import { Tracing } from '@aws-cdk/aws-lambda';

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    var db = new CfnDatabase(this, "tsdb", {
      databaseName: "teslacanbus",
    } )

    const layerArn = "arn:aws:lambda:"+ process.env.CDK_DEFAULT_REGION +":580247275435:layer:LambdaInsightsExtension:2";
    const layer = lambda.LayerVersion.fromLayerVersionArn(this, `LayerFromArn`, layerArn);

    const lambdarole = new iam.Role(this, "lambdarole", {assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')});
    lambdarole.addManagedPolicy({managedPolicyArn: 'arn:aws:iam::aws:policy/CloudWatchLambdaInsightsExecutionRolePolicy'})
    lambdarole.addManagedPolicy({managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'})
    lambdarole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [ 's3:GetObject' ]
    }))
    
    lambdarole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [ "*" ],
      actions: [  
        "timestream:Describe*",
        "timestream:List*",
        "timestream:CreateTable",
        "timestream:WriteRecords",
      ]
    }))

      var lambdaFunc = new PythonFunction(this, "lambda", {
      entry: '../processing',
      index: 'canmsgtosignals.py',
      runtime: lambda.Runtime.PYTHON_3_8,
      environment: {
        'TSDB_NAME': db.databaseName !,
        'TZ': 'Australia/Melbourne',
        // 'DEBUG': 'true'
      }, 
      timeout: Duration.minutes(15),
      memorySize: 1024,
      tracing: Tracing.ACTIVE,
      layers: [layer],  // add in Lambda Insights layer
      role: lambdarole
    })



  }
}

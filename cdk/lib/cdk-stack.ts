import * as cdk from '@aws-cdk/core';
import { CfnDatabase, CfnTable } from '@aws-cdk/aws-timestream';
import { PythonFunction } from '@aws-cdk/aws-lambda-python';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iam from '@aws-cdk/aws-iam';
import { Duration } from '@aws-cdk/core';

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    var db = new CfnDatabase(this, "tsdb", {
      databaseName: "teslacanbus",
    } )

    // var table = new CfnTable(this, "tsdb_table", {
    //   tableName: "CanSignals",
    //   databaseName: db.databaseName !,
    //   retentionProperties: {
    //     MemoryStoreRetentionPeriodInHours: 8766,
    //     MagneticStoreRetentionPeriodInDays: 73000
    //   }
    // })

    // table.addDependsOn(db)

    var lambdaFunc = new PythonFunction(this, "lambda", {
      entry: '../processing',
      index: 'canmsgtosignals.py',
      runtime: lambda.Runtime.PYTHON_3_8,
      environment: {
        'TSDB_NAME': db.databaseName !,
        // 'TSTABLE_NAME': table.tableName !,
        'TZ': 'Australia/Melbourne',
        // 'DEBUG': 'true'
      }, 
      timeout: Duration.minutes(15),
      memorySize: 1024
    })

    lambdaFunc.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [ 's3:GetObject' ]
    }))

    lambdaFunc.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [ "*" ],
      actions: [  
        "timestream:Describe*",
        "timestream:List*",
        "timestream:CreateTable",
        "timestream:WriteRecords",
      ]
    }))


  }
}

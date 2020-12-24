import * as cdk from '@aws-cdk/core';
import { CfnDatabase, CfnTable } from '@aws-cdk/aws-timestream';
import { PolicyStatement, Effect, Role, ServicePrincipal, ManagedPolicy } from '@aws-cdk/aws-iam';
import { EcsTask, SqsQueue} from '@aws-cdk/aws-events-targets';
import { Cluster, ContainerImage, FargatePlatformVersion, FargateTaskDefinition, LogDriver } from '@aws-cdk/aws-ecs';
import { Queue} from '@aws-cdk/aws-sqs';
import { BlockPublicAccess, Bucket, ObjectOwnership } from '@aws-cdk/aws-s3';
import { Duration } from '@aws-cdk/core';
import { Trail } from '@aws-cdk/aws-cloudtrail';

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    var db = new CfnDatabase(this, "tsdb", {
      databaseName: "tesla_canlogjs",
    } )

    var queue = new Queue(this, 'sqs-filestoprocess', { queueName: "canlogFilesToProcess" })

    // Define the ECS Fargat Task that will do our processing
    const cluster = new Cluster(this, 'processlogs', { 
      containerInsights: true,
    })

    const taskRole = new Role(this, 'taskrole', { assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com')})
    taskRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      resources: ['*'],
      actions: [ 's3:GetObject' ]
    }))
    taskRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      resources: [ "*" ],
      actions: [  
        "timestream:Describe*",
        "timestream:List*",
        "timestream:CreateTable",
        "timestream:WriteRecords",
      ]
    }))
    taskRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      resources: [ queue.queueArn ],
      actions: [ 
        "sqs:ReceiveMessage", 
        "sqs:GetQueueAttributes", 
        "sqs:GetQueueUrl",
        "sqs:DeleteMessage"
      ]
    }))
    taskRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      resources: ['*'],
      actions: [ 
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvent",
      ]
    }))
    taskRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"))


    const taskDefinition = new FargateTaskDefinition(this, "process-logs-s3-to-db", {
      memoryLimitMiB: 2048,
      cpu: 1024,
      taskRole: taskRole,
    })
    taskDefinition.addContainer('processing_container', { 
      image: ContainerImage.fromAsset('../processing/'), 
      environment: {
         'TSDB_NAME': db.databaseName !,
         'TZ': 'Australia/Melbourne',
         'SQS_QUEUE_URL': queue.queueUrl
      }, 
      logging: LogDriver.awsLogs({streamPrefix: "ecsprocessing", logRetention: 90})
    })

    const platformVersion = FargatePlatformVersion.VERSION1_4
    const ecsTaskTarget = new EcsTask({ 
      cluster: cluster, 
      taskDefinition: taskDefinition, 
      role: taskRole, 
      platformVersion: platformVersion, 
      taskCount: 1})
    const sqsTarget = new SqsQueue(queue, {})

    var s3source = new Bucket(this, "canlog", { 
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_PREFERRED,
      lifecycleRules: [ { 
        enabled: true,
        abortIncompleteMultipartUploadAfter: Duration.days(1),
        expiration: Duration.days(14),
      } ],
     })

     var trail = new Trail(this, 'Bucket CloudTrail')
     trail.logAllS3DataEvents()

    //var s3source = s3.Bucket.fromBucketName(this, 'bucket', 'badqueen-video-archive')

    s3source.onCloudTrailWriteObject('s3write-starttask', {
      target: ecsTaskTarget
     })

     s3source.onCloudTrailWriteObject('s3write-toqueue', {
      target: sqsTarget
     })
  }
}

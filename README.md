# streaming-analytics

This project sets up the starter AWS infrastructure for Kinesis streaming.

## Record aggregation

Records are produced via KPL (Kinesis Producer Library) will be aggregated if the `recordMaxBufferedTime` property is set. If this is the case, or if you know the records are aggregated, please complete the setup for the deaggregator processor. Details below

If records are not aggregated, you can skip the preprocessor set up. When launching the Kinesis App stack template, set the `SourceRecordsAggregation` option to `false`


**`PreProcessor`**

The preprocessor is a lambda function that de-aggregates KPL records. It is contained in the `ks-preprocessor` directory. The source code in this directory is provided by AWS and is only included here for purposes of Cloudformation automation.
Please find the source here: https://github.com/amazon-archives/serverless-app-examples/tree/master/python/kinesis-analytics-process-kpl-record

Dependencies were added manually by navigating into `/<..>/ks-preprocessor` and running this command from inside the directory:

```
pip install -r requirements.txt -t ./

```

Create a Zip file:

```
zip -r ks-preprocessor.zip .

```

The lambda function can also be deployed manually via the Kinesis Application wizard (by selecting a Lambda temmplate)

The preprocessor function needs to be set up only ONCE. The function can be reused with any other aggregated stream.

**`AWS Cloudformation Templates`**

1. `analytics_bucket.yaml` - Stack template that creates an S3 bucket which will be used to store the output of Kinesis Analytics processing
2. `code-bucket.yaml` - Stack template that creates an S3 bucket for holding the `preprocessor` archive (zip). The zip file is created by packing the contents of the `ks-preprocessor` directory and uploading it to S3 (see the preprocessor section above)
3. `input-stream.yaml` - Stack template that creates a Kinesis Data Stream. Use this template to create data streams for raw KPL Records
4. `preprocessor.yaml` - Stack template that creates a lambda preprocessor function. The function is created from the Zip file contained in the S3 bucket from `code-bucket.yaml`.
5. `streaming-app.yaml` - Stack template that creates a Kinesis Analytics application. This template contains basic aggregation for a single use case. Extend this template to create different applcations for different processing and output

### Running the templates

The templates are highly interdependent. Some stacks use the Outputs of existing stacks and hence those stacks must be run first. See below for dependence graph

WITH RECORD AGGREGATION

```

    analytics_bucket.yaml --> code-bucket.yaml
                                              \
                                               \
                                                input-stream.yaml --> preprocessor.yaml --> streaming-app.yaml
                                               /
                                              /
    code-bucket.yaml --> analytics_bucket.yaml

```

NO RECORD AGGREGATION

Note: Set the `SourceRecordsAggregation` option to `false` when executing the `streaming-app.yaml` template

```

    analytics_bucket.yaml --> code-bucket.yaml
                                              \
                                               \
                                                input-stream.yaml --> streaming-app.yaml
                                               /
                                              /
    code-bucket.yaml --> analytics_bucket.yaml

```

Please use relevant and easy to remember values for the fields. These values are used to link stacks together via Output values

1. ProjectName - Use a common name so that it is easy to identify and related all resources created for a project / initiative. Add tags for easy management

2. StackName - Use a value that gives a general description of function of the stack. The stack name is used to prefix output values for the stack `{StackName}:InputStream`

Apply due diligence when populating the fields. Typos or non-existent resources will lead to failure during creation

### Test the Lambda preprocessor

Use the `testRecord.json` file to check that the preprocessor has been created correctly. Click `Test` on the Lambda Console to create a test and copy the contents of the file into the payload. If successful, the Execution tab should show two records processed successfully

### Validate the application

If successfully created, the Analytics Application will comprise these elements:

1. Source - Streaming data source that reads data from a raw Kinesis data stream, an in-application stream and lambda preprocessor

2. Real-time analytics - A Kinesis SQL application with destination stream(s) and pumps

3. Destination - A firehose delivery stream to S3

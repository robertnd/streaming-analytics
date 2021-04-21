from __future__ import print_function

from aws_kinesis_agg.deaggregator import deaggregate_record
import base64, json


def construct_response_record(record_id, data, is_parse_success):
    return {
        'recordId': record_id,
        'result': 'Ok' if is_parse_success else 'ProcessingFailed',
        'data': base64.b64encode(data)}


def process_kpl_record(kpl_record):
    raw_kpl_record_data = base64.b64decode(kpl_record['data'])
    try:
        # Concatenate the data from de-aggregated records into a single output payload.
        records = deaggregate_record(raw_kpl_record_data)
        newRecords = []
        
        for i in range(0, len(records)):
            recordStr = records[i]
            revenue = {}
            requestDetails = {}
            record = json.loads(recordStr)
            
            for item in record["revenue"]:
                revenue[item["key"]] = item["value"]
            
            for item in record['requestDetails']:
                requestDetails[item['key']] = item['value']

            del record['revenue']
            del record['requestDetails']
            record['revenue'] = revenue
            record['requestDetails'] = requestDetails
            recordStr = json.dumps(record)
            newRecords.append(recordStr)
            
        output_data = "".join(newRecords)
        return construct_response_record(kpl_record['recordId'], output_data, True)
    except BaseException as e:
        print('Processing failed with exception:' + str(e))
        return construct_response_record(kpl_record['recordId'], raw_kpl_record_data, False)


def lambda_handler(event, context):
    '''A Python AWS Lambda function to process aggregated records sent to KinesisAnalytics.'''
    raw_kpl_records = event['records']
    output = [process_kpl_record(kpl_record) for kpl_record in raw_kpl_records]

    # Print number of successful and failed records.
    success_count = sum(1 for record in output if record['result'] == 'Ok')
    failure_count = sum(1 for record in output if record['result'] == 'ProcessingFailed')
    print('Processing completed.  Successful records: {0}, Failed records: {1}.'.format(success_count, failure_count))

    return {'records': output}
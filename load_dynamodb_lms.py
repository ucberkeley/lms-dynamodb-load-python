
import asyncio
import contextlib
import csv
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from argparse import ArgumentParser as Ap, Namespace
from typing import Any

import aioboto3
import aiofiles
import boto3
from aiocsv import AsyncDictReader
from botocore.exceptions import ClientError, ParamValidationError

# keep these constants
AWS_REGION = 'us-west-2'
TABLE_NAME: str = 'lms_assignments'
POS_INT_REGX: str = r'^\d+$'
ERROR_HELP_STRINGS = {
    # Operation specific errors
    'ConditionalCheckFailedException':          'Condition check specified in the operation failed,'
                                                ' review and update the condition check before retrying',
    'TransactionConflictException':             'Operation was rejected because there is an ongoing transaction for'
                                                ' the given, generally safe to retry with exponential back-off',
    'ItemCollectionSizeLimitExceededException': 'A given collection is too large, you\'re using Local Secondary Index'
                                                ' and exceeded size limit of items per partition key.'
                                                ' Consider using Global Secondary Index instead',
    # Common Errors
    'AccessDeniedException':                    'Configure identity based access before retrying',
    'InternalServerError':                      'Internal Server Error,'
                                                ' generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException':   'Request rate is too high. If you\'re using a custom retry strategy'
                                                ' make sure to retry with exponential back-off. Otherwise consider'
                                                ' reducing frequency of requests or increasing provisioned capacity'
                                                ' for your table or secondary index',
    'ResourceNotFoundException':                'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable':                       'Had trouble reaching DynamoDB. generally safe to retry with'
                                                ' exponential back-off',
    'ThrottlingException':                      'Request denied due to throttling, generally safe to retry with'
                                                ' exponential back-off',
    'UnrecognizedClientException':              'The request signature is incorrect most likely due to an'
                                                ' invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException':                      'The input fails to satisfy the constraints specified by DynamoDB,'
                                                ' fix input before retrying',
    'RequestLimitExceeded':                     'Throughput exceeds the current throughput limit for your account,'
                                                ' increase account level throughput before retrying',
}

# these constants are for running this on a dev host
HOME_DIR = os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir))
RESULT_DIR = f'{HOME_DIR}/results/'

global args, dynamodb, logger


#  functions
def parse_arguments() -> Namespace:
    """
    Returns a dictionary containing all arguments provided on the command line
    :return: dict
    """

    parser = Ap(
        description="Adds data from a specified csv file to the LMS DynamoDB table"
    )
    parser.add_argument(
        'file',
        type=str,
        help="The file containing csv data"
    )
    parser.add_argument(
        '-D', '--debug',
        action='store_true',
        help='Set logging level to DEBUG '
             '(default is INFO)'
    )

    arguments: dict[str, Any] = vars(parser.parse_args())

    if "file" not in arguments.keys():
        parser.error("You must specify a file")

    if "file" in arguments.keys() is not None and not os.path.isfile(arguments["file"]):
        parser.error("The specified file does not exist")

    if "file" in arguments.keys() is not None and not str(arguments["file"]).endswith('.csv'):
        parser.error("You must specify a csv file")

    return parser.parse_args()


def convert_date(date: str) -> str:
    if date is None or date == '':
        return ''
    else:
        return dt.datetime.strptime(date, '%m/%d/%y').isoformat()


def convert_nullable(value: str) -> str:
    return value if value is not None and value != '' else 'null'


def convert_orgcode(code: str) -> str:
    return code.replace('01HD', '')


def log_boto_client_error(error) -> None:
    error_code = error.response['Error']['Code']
    error_help_string = ERROR_HELP_STRINGS[error_code]
    logger.exception('[%s] %s.\n\tError message: %s', error_code, error_help_string, error.response['Error']['Message'])


def build_item(given: dict) -> dict:
    global logger
    item: dict = None

    if re.fullmatch(POS_INT_REGX, given['Username']):  # only process items representing persons
        try:
            item = {
                "lms_user_id": int(given['Username']),
                "activity_id": int(given['ActivityIDSource']),
                "calnet_uid": given['SourceIDEmpPk'],
                "empl_id": convert_nullable(given['LocalEmployeeID']),
                "full_name": given['EmpFullName1'],
                "given_name": given['FirstName'],
                "family_name": given['LastName'],
                "empl_org_code": convert_orgcode(given['UserPrimaryOrganizationCode']),
                "manager_empl_id": convert_nullable(given['ManagerLocalEmployeeID']),
                "activity_code": given['ActivityCode'],
                "activity_name": given['ActivityName'],
                "is_required": bool(given['AssignmentStatus'] == 'Required'),
                "assignment_status": given['UCRequirementStatus'],
                "assigned_date": convert_date(given['PlanDate']),
                "due_date": convert_date(given['DueDate']),
                "expiration_date": convert_date(given['ExpirationDate']),
                "last_attempt_date": convert_date(given['AttemptEndDate']),
                "last_completion_date": convert_date(given['LastCompletionDateRealtime']),
                "last_updated_datetime": dt.datetime.now().isoformat(timespec='seconds'),
                "last_updated_event_id": 0
            }

        except Exception as e:
            logger.debug('Exception while building item: %s', e)
            raise e

    return item


async def load_from_csv() -> int:
    global dynamodb, logger
    item_count: int = 0

    table = await dynamodb.Table(TABLE_NAME)

    async with aiofiles.open(args.file, mode='r', encoding='utf-8', errors='replace') as f:
        reader: AsyncDictReader[str] = AsyncDictReader(f)

        async with table.batch_writer() as writer:
            writes = []

            async for this in reader:
                try:
                    item = build_item(this)
                    if item:
                        future = asyncio.ensure_future(writer.put_item(Item=item))
                        writes.append(future)
                        item_count += 1
                        await asyncio.gather(*writes)

                    if item_count % 100 == 0:
                        logger.debug('Loaded %s items', item_count)

                    # no need to load all 180k for method comparison
                    if item_count == 5000:
                        item_count = item_count
                        break

                except ValueError as e:
                    logger.exception('Invalid value while putting item: %s\n%s', item, e)
                except ParamValidationError as e:
                    logger.exception('Invalid parameter while putting item: %s\n%s', item, e)
                except ClientError as e:
                    log_boto_client_error(e)
                    return item_count
                except Exception as e:
                    logger.exception('Unknown error during load_from_csv: %s', e)
                    return item_count

    return item_count


async def main() -> None:
    global args, dynamodb, logger

    args = parse_arguments()

    # remove this block once automated
    if not os.path.exists(RESULT_DIR):
        os.makedirs(RESULT_DIR)
    results_file = RESULT_DIR + 'load_dynamodb_lms_' + dt.datetime.now().strftime('%Y%m%d%H%M') + '.txt'
    # end of optional block

    # set logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(filename=results_file, mode='a'),  # this line can be removed w results file block
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # begin
    start_time = time.time()
    logger.info('Started')

    try:
        # create a durable aws session
        stack = contextlib.AsyncExitStack()
        data_session = aioboto3.Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            aws_session_token=os.environ['AWS_SESSION_TOKEN']
        )
        async with data_session.resource('dynamodb', region_name=AWS_REGION) as dynamodb:
            item_count: int = await load_from_csv()

    except FileNotFoundError as e:
        logger.exception('Unable to find specified file (%s): %s', args.file, e)
    except csv.Error as e:
        logger.exception('Unable to read CSV file (%s): %s', args.file, e)
    except IOError as e:
        logger.exception('Unexpected IO problem: %s', e)
    except ClientError as e:
        log_boto_client_error(e)
    except Exception as e:
        logger.exception('Unknown problem: %s', e)

    await stack.aclose()

    logger.info('Loaded %s items in %s', item_count, dt.timedelta(seconds=time.time()-start_time))


# main
if __name__ == "__main__":
    asyncio.run(main())
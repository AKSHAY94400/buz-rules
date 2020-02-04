import json
import os
import traceback

from kafka import KafkaConsumer, TopicPartition
from random import randint
from time import sleep

from db_utils import DB
from producer import produce
from ace_logger import Logging
from apply_business_rule import apply_business_rule

logging = Logging()


# main module


db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}

def create_consumer(route, broker_url='broker:9092'):
    consumer = KafkaConsumer(
                    bootstrap_servers=broker_url,
                    value_deserializer=lambda value: json.loads(value.decode()),
                    auto_offset_reset='earliest',
                    group_id=route,
                    api_version=(0,10,1),
                    enable_auto_commit=False,
                    session_timeout_ms=800001,
                    request_timeout_ms=800002
    )

    return consumer

def consume(broker_url='broker:9092'):
    try:
        route = 'run_business_rule'
        logging.info(f'Listening to topic: {route}')

        consumer = create_consumer(route)
        logging.debug('Consumer object created.')

        parts = consumer.partitions_for_topic(route)
        if parts is None:
            logging.warning(f'No partitions for topic `{route}`')
            logging.debug(f'Creating Topic: {route}')
            produce(route, {})
            print(f'Listening to topic `{route}`...')
            while parts is None:
                consumer = create_consumer(route)
                parts = consumer.partitions_for_topic(route)
                logging.warning("No partition. In while loop. Make it stop")

        partitions = [TopicPartition(route, p) for p in parts]
        consumer.assign(partitions)

        for message in consumer:
            data = message.value
            logging.info(f'Message: {data}')

            try:
                case_id = data['case_id']
                functions = data.pop('functions', None)
                tenant_id = data['tenant_id']
                start_rule_id = data.pop('start_rule_id', None)
                stage = data.get('stage',None)
                workflow = data.get('workflow', None)
                file_name = data.get('file_name',None)
                files = data.get('files',None)
                source = data.get('source',None)
                file_path = data.get('file_path',None)
                original_file_name = data.get('original_file_name',None)
            except Exception as e:
                logging.warning(f'Recieved unknown data. [{data}] [{e}]')
                consumer.commit()
                continue
            
            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)
            kafka_db = DB('kafka', **db_config)

            query = 'SELECT * FROM `button_functions` WHERE `route`=%s'
            function_info = queue_db.execute(query, params=[route])
            in_progress_message = list(function_info['in_progress_message'])[0]
            failure_message = list(function_info['failure_message'])[0]
            success_message = list(function_info['success_message'])[0]

            message_flow = kafka_db.get_all('grouped_message_flow')

            # Get which button (group in kafka table) this function was called from
            group = data.pop('group', None)
            if group:
                # Get message group functions
                group_messages = message_flow.loc[message_flow['message_group'] == group]

                # If its the first function the update the progress count
                first_flow = group_messages.head(1)
                first_topic = first_flow.loc[first_flow['listen_to_topic'] == route]
                
                query = 'UPDATE `process_queue` SET `status`=%s, `total_processes`=%s, `case_lock`=1 WHERE `case_id`=%s'
                
                if not first_topic.empty:
                    logging.debug(f'`{route}` is the first topic in the group `{group}`.')
                    logging.debug(f'Number of topics in group `{group}` is {len(group_messages)}')
                    if list(first_flow['send_to_topic'])[0] is None:
                        queue_db.execute(query, params=[in_progress_message, len(group_messages), case_id])
                    else:
                        queue_db.execute(query, params=[in_progress_message, len(group_messages) + 1, case_id])

            # Getting the correct data for the functions. This data will be passed through
            # rest of the chained functions.
            function_params = {}
            if functions:
                for function in functions:
                    if function['route'] == route:
                        function_params = function['parameters']
                        break
            else:
                function_params['stage'] = [stage]

            # Call the function
            try:
                logging.debug(f'Calling function `run_business_rule`')
                result = apply_business_rule(case_id, function_params, tenant_id,  start_rule_id)
                logging.debug(f'Ran business rules with stage {stage}')
                logging.debug(f"result flag is {result.get('flag', '')}")
            except:
                # Unlock the case.
                logging.exception(f'Something went wrong while saving changes. Check trace.')
                query = 'UPDATE `process_queue` SET `status`=%s, `case_lock`=0, `failure_status`=1 WHERE `case_id`=%s'
                queue_db.execute(query, params=[failure_message, case_id])
                consumer.commit()
                continue

            # Check if function was succesfully executed
            if group:
                if result['flag']:
                    # If there is only function for the group, unlock case.
                    if not first_topic.empty:
                        if list(first_flow['send_to_topic'])[0] is None:
                            # It is the last message. So update file status to completed.
                            query = 'UPDATE `process_queue` SET `status`=%s, `case_lock`=0, `completed_processes`=`completed_processes`+1 WHERE `case_id`=%s'
                            queue_db.execute(query, params=[success_message, case_id])
                            consumer.commit()
                            continue

                    last_topic = group_messages.tail(
                        1).loc[group_messages['send_to_topic'] == route]
                    
                    # If it is not the last message, then produce to next function else just unlock case.
                    if last_topic.empty:
                        # Get next function name
                        next_topic = list(
                            group_messages.loc[group_messages['listen_to_topic'] == route]['send_to_topic'])[0]

                        if next_topic is not None:
                            logging.debug('Not the last topic of the group.')
                            produce(next_topic, data)

                        # Update the progress count by 1
                        query = 'UPDATE `process_queue` SET `status`=%s, `completed_processes`=`completed_processes`+1 WHERE `case_id`=%s'
                        queue_db.execute(query, params=[success_message, case_id])
                        consumer.commit()
                    else:
                        # It is the last message. So update file status to completed.
                        logging.debug('Last topic of the group.')
                        query = 'UPDATE `process_queue` SET `status`=%s, `case_lock`=0, `completed_processes`=`completed_processes`+1 WHERE `case_id`=%s'
                        queue_db.execute(query, params=[success_message, case_id])
                        consumer.commit()
                else:
                    # Unlock the case.
                    logging.debug('Flag false. Unlocking case with failure status 1.')
                    query = 'UPDATE `process_queue` SET `status`=%s, `case_lock`=0, `failure_status`=1 WHERE `case_id`=%s'
                    queue_db.execute(query, params=[failure_message, case_id])
                    consumer.commit()
            else:
                data = result['send_data'] if 'send_data' in result else {}
                data['case_id'] = case_id
                data['tenant_id'] = tenant_id
                data['workflow'] = workflow
                data['type'] = 'file_ingestion'
                data['file_name'] = file_name
                data['files'] = files
                data['source'] = source
                data['file_path'] = file_path
                data['original_file_name'] = original_file_name

                query = 'SELECT * FROM `message_flow` WHERE `listen_to_topic`=%s AND `workflow`=%s'
                logging.debug(f'topic - {route} , workflow - {workflow}')
                message_flow = kafka_db.execute(query, params=[route, workflow])

                if message_flow.empty:
                    logging.error('`kafka table` is not configured correctly in message flow table.')
                    send_to_topic = None
                else:
                    send_to_topic = list(message_flow.send_to_topic)[0]

                if send_to_topic is not None:
                    logging.info(f'Producing to topic {send_to_topic}')
                    produce(send_to_topic, data)
                else:
                    logging.info(f'There is no topic to send.')
                if 'send_to_topic' in result:
                    send_to_topic_bypassed = result['send_to_topic']
                    produce(send_to_topic_bypassed, {})
                else:
                    logging.error('Message not consumed. Some error must have occured. Will try again!')
                consumer.commit()
    except:
        logging.exception('Something went wrong in consumer. Check trace.')

if __name__ == '__main__':
    consume()

from faker import Faker
import pytz
import random
import json
from tqdm import tqdm
import time
import os

fake = Faker()
Faker.seed(2023)

log_keys = []
modules = []
actions = ["CREATE", "READ", "UPDATE", "DELETE"]
address_types = ["IP ADDRESS IPv4", "IP ADDRESS IPv6", "MAC ADDRESS"]
log_levels = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
channels = ["MOBILE", "WEB", "DESKTOP"]
smartphones = []
browsers = [
    "Google Chrome",
    "Mozilla Firefox",
    "Apple Safari",
    "Microsoft Edge",
    "Opera",
    "Internet Explorer"
]
desktops = [
    "Windows PC",
    "Mac",
    "Linux",
    "Ubuntu",
    "Fedora",
    "Chrome OS",
    "FreeBSD",
    "Debian",
    "CentOS",
    "Mint",
    "OpenSUSE",
    "Arch Linux"
]

with open("/data-choices/log-keys.txt", "r") as f:
    log_keys = f.readlines()
f.close
log_keys = [log_key.rstrip('\n') for log_key in log_keys]

with open("/data-choices/activity-modules.txt", "r") as f:
    modules = f.readlines()
f.close
modules = [module.rstrip('\n') for module in modules]

with open("/data-choices/smartphones.txt", "r") as f:
    smartphones = f.readlines()
f.close
smartphones = [smartphone.rstrip('\n') for smartphone in smartphones]

def generate_elements():
    unique_items = set()
    while len(unique_items) < 50:
        item = fake.uuid4()
        unique_items.add(item)
    with open('/data-choices/log-keys.txt', 'w') as f:
        for item in unique_items:
            f.write(item + "\n")
    f.close

def generate_address():
    address_type = fake.random_element(elements=address_types)
    if address_type == "IP ADDRESS IPv4":
        address_value = fake.ipv4()
    elif address_type == "IP ADDRESS IPv6":
        address_value = fake.ipv6()
    else:
        address_value = fake.mac_address()
    
    return address_type, address_value

def generate_timestamp():
    timezones = list(pytz.all_timezones)
    timezone = random.choice(timezones)
    timestamp = fake.date_time_between(start_date='-1y', end_date='now', tzinfo=pytz.timezone(timezone))
    timestamp_str = timestamp.strftime(f'%Y-%m-%d %H:%M:%S.%f %z')
    return timestamp_str

def generate_activity():
    activity = {
        "module": fake.random_element(elements=modules),
        "action": fake.random_element(elements=actions)
    }
    return activity

def generate_channel_device():
    channel = fake.random_element(elements=channels)
    if channel == "MOBILE":
        device = fake.random_element(elements=smartphones)
    elif channel == "WEB":
        device = fake.random_element(elements=browsers)
    else:
        device = fake.random_element(elements=desktops)
    return channel, device

def generate_source():
    address_type, address_value = generate_address()
    channel, device = generate_channel_device()
    source = {
        "channel": channel,
        "address": {
            "type": address_type,
            "value": address_value
        },
        "user": {
			"userId": fake.random_number(digits=6),
			"username": fake.user_name()
		},
		"device": {
			"name": device,
		}
    }
    return source

def generate_destination():
    address_type, address_value = generate_address()
    destination = {
        "channel": fake.word().title(),
		"address": {
			"type": address_type,
			"value": address_value
		},
		"device": {
			"name": fake.word().title(),
		}
    }
    return destination

def generate_random_json(minSize, maxSize):
    random_json = {}
    for i in range(random.randint(minSize, maxSize)):
        random_json[fake.word()] = fake.word()
    return random_json

def generate_status():
    status = {
		"code": fake.random_number(digits=3),
		"message": fake.sentence(),
		"error": {
			"code": fake.random_number(digits=3),
			"message": fake.sentence(),
			"details": generate_random_json(1, 3)
		}
	}
    return status

def generate_changes():
    changes = []
    for i in range(random.randint(1, 3)):
        change = {
            "path": fake.word(),
            "desc": {
                "description": fake.sentence(),
                "displayName": fake.word()
            }
        }
        changes.append(change)
    return changes

def generate_audit_trail():
    entity = f'{fake.word()}_{fake.word()}'.upper()
    entityDesc = entity.replace("_", " " ).title()
    audit_trail = {
		"info":{
			"version": f'{fake.random_number(digits=1)}.{fake.random_number(digits=2)}',
			"entity": entity,
			"entityDesc": entityDesc
		},
		"before": generate_random_json(1, 3),
		"after": generate_random_json(1, 3),
		"changes": generate_changes(),
		"childs": generate_random_json(1, 3)
	}
    return audit_trail

####### Activity Log #######
def generate_audit_log_query_values():
    log_key = str(fake.random_element(elements=log_keys))
    reference_number = str(fake.random_number(digits=8))
    transaction_timestamp = str(generate_timestamp())
    log_level = fake.random_element(elements=log_levels)
    session_id = str(fake.random_number(digits=8))
    correlation_id = str(fake.random_number(digits=8))
    activity = json.dumps(generate_activity())
    source = json.dumps(generate_source())
    destination = json.dumps(generate_destination())
    status = json.dumps(generate_status())
    audit_trail = json.dumps(generate_audit_trail())
    
    query = f'(\'{log_key}\', \'{reference_number}\', \'{transaction_timestamp}\', \'{log_level}\', \'{session_id}\', \'{correlation_id}\', \'{activity}\', \'{source}\', \'{destination}\', \'{status}\', \'{audit_trail}\')'
    
    return query

def generate_audit_log_query(length):
    with open('/sql/audit-log-data.sql', 'w') as f:
        start_time = time.time()
        query = "INSERT INTO tbl_audit_log (log_key, reference_number, transaction_timestamp, log_level, session_id, correlation_id, activity, source, destination, status, audit_trail) VALUES \n"
        f.write(query)
        
        for i in tqdm(range(length), desc="Generating Audit Log SQL query..."):
            if i != (length - 1):
                query = generate_audit_log_query_values() + ",\n"
                f.write(query)
            else:
                query = generate_audit_log_query_values() + ";" 
                f.write(query)
                
        elapsed_time = time.time() - start_time
        f.write(f"\n\n-- Query generated in {elapsed_time:.2f} seconds")
        print(f"Completed in {elapsed_time:.2f} seconds")
    f.close

####### Activity Log #######
def generate_activity_log_query_values():
    log_key = str(fake.random_element(elements=log_keys))
    reference_number = str(fake.random_number(digits=8))
    transaction_timestamp = str(generate_timestamp())
    log_level = fake.random_element(elements=log_levels)
    session_id = str(fake.random_number(digits=8))
    correlation_id = str(fake.random_number(digits=8))
    activity = json.dumps(generate_activity())
    source = json.dumps(generate_source())
    destination = json.dumps(generate_destination())
    status = json.dumps(generate_status())
    
    query = f'(\'{log_key}\', \'{reference_number}\', \'{transaction_timestamp}\', \'{log_level}\', \'{session_id}\', \'{correlation_id}\', \'{activity}\', \'{source}\', \'{destination}\', \'{status}\')'
    
    return query

def generate_activity_log_query(length):
    with open('/sql/activity-log-data.sql', 'w') as f:
        start_time = time.time()
        query = "INSERT INTO tbl_activity_log (log_key, reference_number, transaction_timestamp, log_level, session_id, correlation_id, activity, source, destination, status) VALUES \n"
        f.write(query)
        
        for i in tqdm(range(length), desc="Generating Activity Log SQL query..."):
            if i != (length - 1):
                query = generate_activity_log_query_values() + ",\n"
                f.write(query)
            else:
                query = generate_activity_log_query_values() + ";" 
                f.write(query)
                
        elapsed_time = time.time() - start_time
        f.write(f"\n\n-- Query generated in {elapsed_time:.2f} seconds")
        print(f"Completed in {elapsed_time:.2f} seconds")
    f.close

# generate_audit_log_query(1000000)
# generate_activity_log_query(1000000)

def get_query_header(table_name):
    if table_name == 'activity-log':
        query_header = "INSERT INTO tbl_activity_log (log_key, reference_number, transaction_timestamp, log_level, session_id, correlation_id, activity, source, destination, status) VALUES \n"
    elif table_name == 'audit-log':
        query_header = "INSERT INTO tbl_audit_log (log_key, reference_number, transaction_timestamp, log_level, session_id, correlation_id, activity, source, destination, status, audit_trail) VALUES \n"
    else:
        query_header = 'deez'
    
    return query_header

# write the query into multiple files (query length, max file size(MB), table name)
def write_into_multiple_files(length, max_file_size, table_name):
    if table_name == 'activity-log':
        filename = 'sql/activity-log-data'
    elif table_name == 'audit-log':
        filename = 'sql/audit-log-data'
    else:
        filename = 'deez'
    
    file_index = 1
    max_file_size *= 1024 * 1024
    hasReset = True
    
    query_header = get_query_header(table_name)
    
    for i in tqdm(range(length), desc=f"Generating {table_name} SQL query..."):
        current_filename = f'{filename}-{file_index}.sql'
        
        with open(current_filename, "a") as file:
            current_filesize = os.path.getsize(current_filename)
            if hasReset == True:
                file.write(query_header)
                hasReset = False
            
            if table_name == 'activity-log':
                query_body = generate_activity_log_query_values()
            elif table_name == 'audit-log':
                query_body = generate_audit_log_query_values()
            else:
                query_body = 'deez'
            
            file.write(query_body)
            
            if current_filesize >= max_file_size or i == length - 1:
                file.write(";") # end the query
                hasReset = True
                file_index += 1
            else:
                file.write(",\n") # continue the query

write_into_multiple_files(1000000, 40, "activity-log")
write_into_multiple_files(1000000, 40, "audit-log")
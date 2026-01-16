import uuid
import random
import time
from datetime import datetime,timedelta
import json

Event_types=["app_open","button_click","page_view","purchase"]
Devices=["Mobile","web"]
Countries=["IN","US","UK","JP"]
Events_Per_Second=5

Duplicate_Probability=0.05
Late_Event_Probability=0.10

def generate_event():
    event_time=datetime.utcnow()
    if random.random()<Late_Event_Probability:
        event_time-=timedelta(seconds=random.randint(5,120))
    
    event={
        "event_id":str(uuid.uuid4()),
        "user_id":str(random.randint(1,1000)),
        "event_type":random.choice(Event_types),
        "event_time":event_time.isoformat()+"z",
        "ingest_time":datetime.utcnow().isoformat()+"z",
        "device_type":random.choice(Devices),
        "country":random.choice(Countries),
        "Session_id":f"s-{random.randint(1000,9999)}",
        "meta_data":{
            "app_version":f"{random.randint(1,5)}.{random.randint(0,9)}",
            "Screen":random.choice(["Home","Profile","Settings","Shop","Search"]),
        }
    }
    return event

def event_stream():
    last_event=None
    while True:
        event=generate_event()
        
        if last_event and random.random()<Duplicate_Probability:
            event=last_event
        
        print(json.dumps(event))
        last_event=event
        time.sleep(1/Events_Per_Second)
if __name__=="__main__":
    event_stream()       
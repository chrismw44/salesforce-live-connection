import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
import re
from credentials import *
from refreshTableauData import *
from queries import *
from errorLogging import *
from util.ChangeEventHeaderUtility import process_bitmap


#Set initial variables
access_token = None
expired_error = False

while True:
    try:
        if access_token is None or expired_error:
            access_token = None
            access_token = get_salesforce_access_token(PRODUCTION_USERNAME)
            if access_token:
                print("Access token received.")
            else:
                print("Failed to obtain access token.")

        #Initial tableau datasource refresh - Get, transform & join Event and Event Colleague data
        process_event_data(ENDPOINT, queryEvent, queryEventContacts, queryEventColleague, access_token)
        


        semaphore = threading.Semaphore(1)
        latest_replay_id = None


        with open(certifi.where(), 'rb') as f:
            creds = grpc.ssl_channel_credentials(f.read())
        with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
            
            username = PRODUCTION_USERNAME
            password = PRODUCTION_PASSWORD_TOKEN
            url = PRODUCTION_LOGIN_URL
            headers = {'content-type': 'text/xml', 'SOAPAction': 'login'}
            xml = f"""<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/'
            xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
            xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>
            <urn:login>
            <urn:username><![CDATA[{username}]]></urn:username>
            <urn:password><![CDATA[{password}]]></urn:password>
            </urn:login></soapenv:Body></soapenv:Envelope>"""
            res = requests.post(url, data=xml, headers=headers, verify=False)
            sessionid = re.search(r'<sessionId>(.*?)</sessionId>', res.content.decode('utf-8')).group(1)


            instanceurl = PRODUCTION_INSTANCEURL
            tenantid = PRODUCTION_TENANTID
            authmetadata = (('accesstoken', sessionid),
            ('instanceurl', instanceurl),
            ('tenantid', tenantid))


            stub = pb2_grpc.PubSubStub(channel)


            def fetchReqStream(topic):
                while True:
                    semaphore.acquire()
                    yield pb2.FetchRequest(
                        topic_name = topic,
                        replay_preset = pb2.ReplayPreset.LATEST,
                        num_requested = 1)
                   
            def decode(schema, payload):
                schema = avro.schema.parse(schema)
                buf = io.BytesIO(payload)
                decoder = avro.io.BinaryDecoder(buf)
                reader = avro.io.DatumReader(schema)
                ret = reader.read(decoder)
                return ret
           
            mysubtopic = "/data/AttendanceMeetings_Channel__chn"
            print('Subscribing to ' + mysubtopic)
            substream = stub.Subscribe(fetchReqStream(mysubtopic), metadata=authmetadata)
            for event in substream:
                if event.events:
                    semaphore.release()
                    print("Number of events received: ", len(event.events))
                    payloadbytes = event.events[0].event.payload
                    schemaid = event.events[0].event.schema_id
                    schema = stub.GetSchema(
                            pb2.SchemaRequest(schema_id=schemaid),
                            metadata=authmetadata).schema_json
                    decoded = decode(schema, payloadbytes)
                    changed_fields = decoded['ChangeEventHeader']['changedFields']
                    print("Change Type: " + decoded['ChangeEventHeader']['changeType'])
                    print("=========== Changed Fields =============")
                    print(process_bitmap(avro.schema.parse(schema), changed_fields))
                    print("=========================================")
                    print("Got an event!", json.dumps(decoded))


                    #Get, transform & join Event and Event Colleague data
                    
                    process_event_data(ENDPOINT, queryEvent, queryEventContacts, queryEventColleague, access_token)
                    


                else:
                    print("[", time.strftime('%b %d, %Y %I:%M%p %Z'),
                    "] The subscription is active.")
                latest_replay_id = event.latest_replay_id


    except Exception as e:
        log_error("Error in main loop: " + str(e))
        expired_error = "Session expired" in str(e)
        time.sleep(60)  # Wait for 60 seconds before retrying






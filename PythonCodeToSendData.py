import json
import Adafruit_DHT
import random
import time
import sys
import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
from iothub_client import IoTHubClientRetryPolicy, GetRetryPolicyReturnValue
from iothub_client_args import get_iothub_opt, OptionError

sensor=Adafruit_DHT.DHT11
 
#set the gpio pin for the input 
gpio=17
 
# Use read_retry method. This will retry up to 15 times to
# get a sensor reading (waiting 2 seconds between each retry).
humidity1, temperature1 = Adafruit_DHT.read_retry(sensor, gpio)

temp=temperature1
hum=humidity1


TIMEOUT = 241000
MINIMUM_POLLING_TIME = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.

MESSAGE_TIMEOUT = 10000

AVG_CURRENT = 10
AVG_LOAD = 5
RECEIVE_CONTEXT = 0
AVG_WIND_SPEED = 10
AVG_TEMPERATURE = 20
AVG_HUMIDITY = 60


AVG_SPEED=10
AVG_DISTANCE=20
AVG_FUEL=30
AVG_COOLANT=40
AVG_TORQUE=50


MESSAGE_COUNT = 10000
RECEIVED_COUNT = 0
CONNECTION_STATUS_CONTEXT = 0
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
CONNECTION_STATUS_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0

# chose HTTP, AMQP, AMQP_WS or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.MQTT

CONNECTION_STRING = "HostName=smart-ar-inspector-iothub.azure-devices.net;DeviceId=MyDotnetDevice;SharedAccessKey=RA//FunD4LyLSxiO+9GcFE4xLgaU1bTCZRzz+TWuUkc="

#MSG_TXT = "{\"deviceId\": \"myPythonDevice\",\"windSpeed\": %d,\"temperature\": %d,\"humidity\": %d}"

# some embedded platforms need certificate information


def set_certificates(client):
    from iothub_client_cert import CERTIFICATES
    try:
        client.set_option("TrustedCerts", CERTIFICATES)
        print ( "set_option TrustedCerts successful" )
    except IoTHubClientError as iothub_client_error:
        print ( "set_option TrustedCerts failed (%s)" % iothub_client_error )


def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    print ( "Received Message [%d]:" % counter )
    print ( "    Data: <<<%s>>> & Size=%d" % (message_buffer[:size].decode('utf-8'), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
   # print ( "    message_id: %s" % message.message_id )
   #print ( "    correlation_id: %s" % message.correlation_id )
   #key_value_pair = map_properties.get_internals()
   #print ( "    Properties: %s" % key_value_pair )
   #SEND_CALLBACKS += 1
   #print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )


def connection_status_callback(result, reason, user_context):
    global CONNECTION_STATUS_CALLBACKS
    print ( "Connection status changed[%d] with:" % (user_context) )
    print ( "    reason: %d" % reason )
    print ( "    result: %s" % result )
    CONNECTION_STATUS_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % CONNECTION_STATUS_CALLBACKS )


def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    print ( "")
    print ( "Twin callback called with:")
    print ( "updateStatus: %s" % update_state )
    print ( "context: %s" % user_context )
    print ( "payload: %s" % payload )
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    print ( "Confirmation[%d] for reported state received with:" % (user_context) )
    print ( "    status_code: %d" % status_code )
    SEND_REPORTED_STATE_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS
    print ( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    return device_method_return_value


def iothub_client_init():
    # prepare iothub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # some embedded platforms need certificate information
    set_certificates(client)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    if client.protocol == IoTHubTransportProvider.AMQP or client.protocol == IoTHubTransportProvider.AMQP_WS:
        client.set_connection_status_callback(
            connection_status_callback, CONNECTION_STATUS_CONTEXT)

    retryPolicy = IoTHubClientRetryPolicy.RETRY_INTERVAL
    retryInterval = 100
    client.set_retry_policy(retryPolicy, retryInterval)
    print ( "SetRetryPolicy to: retryPolicy = %d" %  retryPolicy)
    print ( "SetRetryPolicy to: retryTimeoutLimitInSeconds = %d" %  retryInterval)
    retryPolicyReturn = client.get_retry_policy()
    print ( "GetRetryPolicy returned: retryPolicy = %d" %  retryPolicyReturn.retryPolicy)
    print ( "GetRetryPolicy returned: retryTimeoutLimitInSeconds = %d" %  retryPolicyReturn.retryTimeoutLimitInSeconds)

    return client


def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        print ( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        print ( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            print ( "No message received" )
        else:
            print ( iothub_client_error )


def iothub_client_sample_run():

    try:

        client = iothub_client_init()

        if client.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)

        while True:

            # send a few messages every minute
            print ( "IoTHubClient sending %d messages" % MESSAGE_COUNT )

            for message_counter in range(0, MESSAGE_COUNT):

                #BIKE DATA
                
                bike="Bike"

                Speed = int(AVG_SPEED + (random.random() * 4 + 2))
                Distance = int(AVG_DISTANCE + (random.random() * 2 + 2))
                Fuel = int(hum)

                    
                
                #CAR DATA

                car="Car"

                Temperature2=int(temp)
                Distance2 = int(AVG_DISTANCE + (random.random() * 7 + 8))
                coolant = int(AVG_COOLANT + (random.random() * 3 + 2))
                         
                #for truck

                truck="Truck"
                
                Temperature3=int(temp)
                Speed3 = int(AVG_SPEED + (random.random() * 10 + 10))
                Fuel3 = int(hum*2)
                Torque3 = int(AVG_TORQUE + (random.random() * 5 + 2))



                auto="Auto"
                
                Power4=int(temp*3)
                Torque4 = int(AVG_TORQUE + (random.random() * 5 + 2)) 
                Coolant4 = int(AVG_COOLANT + (random.random() * 4 + 7))
                  
                
                #instead of the formatted text i will put the temperature,humidity and wind_speed inside the dictionary and then serialize it 

                telemetryDictionary={
                    
                   "IdMachine1"      : bike,
                   "Speed"           : Speed,
                   "Distance"        : Distance,
                   "Fuel"            : Fuel,
                   

                   
                   

                   "IdMachine2"      : car,
                   "Temperature2"    : Temperature2,
                   "Distance2"       : Distance2,
                   "Coolant"         : coolant,
                   


                   "IdMachine3"      : truck,
                   "Temperature3"    : Temperature3,
                   "Speed3"          : Speed3,
                   "Fuel3"           : Fuel3,
                   "Torque3"         : Torque3,


                   
                   "IdMachine4"      : auto,
                   "Power4"          : Power4,
                   "Torque4"         : Torque4,
                   "Coolant4"        : Coolant4
                   


                   
                   
                   
                }

                #now the data is converted to a dictionary now serialize it 

                print('\n')
                print('\n')
                
                print("BIKE   DATA  :- \n \n ")
                print("SPEED        :  "+str(Speed))
                print("DISTANCE     :  "+str(Distance))
                print("FUEL         :  "+str(Fuel))

                print('\n')
                print('\n')
                
                print("CAR  DATA    :- \n \n ")
                print("Temperature2 :  "+str(Temperature2))
                print("Distance2    :  "+str(Distance2))
                print("Coolant      :  "+str(coolant))

                print('\n')
                print('\n')
                
                print("TRUCK  DATA  :- \n \n ")
                print("SPEED3       :  "+str(Speed3))
                print("Fuel3        :  "+str(Fuel3))
                print("Torque3      :  "+str(Torque3))

                print('\n')
                print('\n')


                print("TRUCK  DATA  :- \n \n ")
                
                print("SPEED3       :  "+str(Speed3))
                print("Fuel3        :  "+str(Fuel3))
                print("Torque3      :  "+str(Torque3))


                print("AUTORIKSHAW  DATA  :- \n \n ")
                
                print("Power4       :  "+str(Power4))
                print("Torque4       :  "+str(Torque4))
                print("Coolant4      :  "+str(Coolant4))
                
                
                jsonmessage=json.dumps(telemetryDictionary)

                

                time.sleep(2)
               # print(msg_txt_formatted)
                #print(jsonmessage)

                # messages can be encoded as string or bytearray
                if (message_counter & 1) == 1:
                    #message = IoTHubMessage(bytearray(msg_txt_formatted, 'utf8'))
                    message = IoTHubMessage(bytearray(jsonmessage, 'utf8'))
                else:
                    message = IoTHubMessage(jsonmessage)
               
                

                client.send_event_async(message, send_confirmation_callback, message_counter)
               
            
            

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

    print_last_message_time(client)



if __name__ == '__main__':
   # CONNECTION_STRING, PROTOCOL = get_iothub_opt(sys.argv[1:], CONNECTION_STRING, PROTOCOL
   
    try:
        (CONNECTION_STRING, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING, PROTOCOL)
    except OptionError as option_error:
        print ( option_error )
        usage()
        sys.exit(1)
        
   
   
    print ( " sending the information to iot hub " )
   #print ( "    Protocol %s" % PROTOCOL )
   #print ( "    Connection string=%s" % CONNECTION_STRING )

    iothub_client_sample_run()




from flask import Flask, render_template, request, render_template_string, jsonify
from flask_sockets import Sockets

import os
import json
import uuid
import logging
import threading
from twilio.twiml.voice_response import VoiceResponse
from twilio.rest import Client
from voice_and_silence_detecting_lex_wrapper import VoiceAndSilenceDetectingLexClient

HTTP_SERVER_PORT = int(os.environ.get('CONTAINER_PORT'))

app = Flask(__name__)
sockets = Sockets(app)

updated_twimls = {}

def log(msg, *args):
    print("Media WS: ", msg, *args)

@app.route("/ping")
def healthCheckResponse():
    return jsonify({"message" : "echo...health check...."})

@app.route('/updatecall', methods=['POST'])
def returnTwimlForCallSid():
    request_object = request.form.to_dict()
    response = updated_twimls[request_object["CallSid"]]

    updated_twimls.pop(request_object["CallSid"])
    return response

@app.route('/twiml', methods=['POST'])
def return_twiml():
    print("POST TwiML")
    return render_template('streams.xml')

@sockets.route('/')
def echo(ws):
    print("Connection accepted")
    client_data_processor = TwilioDataProcessor(ws)
    client_data_processor.start()

class TwilioCall:
    AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
    SERVICE_DNS = os.environ.get('URL')
    
    def __init__(self, account_sid, call_sid):
        self.account_sid = account_sid
        self.auth_token = self.AUTH_TOKEN
        self.service_dns = self.SERVICE_DNS
        self.call_sid = call_sid

    def update(self):
        update_url = "{0}/{1}".format(self.service_dns, "updatecall")
        rest_client = Client(self.account_sid, self.auth_token)
        rest_client.calls(self.call_sid).update(method="POST", url=update_url)

    def persist(self, response):
        updated_twimls[self.call_sid] = response


class TwilioDataProcessor:
    def __init__(self, ws):
        self.logger = logging.getLogger(__name__)
        self.ws = ws
        raw_id = str(uuid.uuid4())
        self.user_id = raw_id[0:24].replace("-", "").upper()
        self.lex_streaming_client = VoiceAndSilenceDetectingLexClient(self.user_id, [self], [self])
        self.listen_switch = threading.Event()
        self.twilio_call = None

    def start(self):
        try:
            while not self.ws.closed:
                while not self.listen_switch.is_set():
                    message = self.ws.receive()
                    if message is None:
                        print('No message')
                        break

                    data = json.loads(message)
                    if data['event'] == "connected":
                        log("Connected Message received", message)
                    if data['event'] == "start":
                        log("Start Message received", message)
                        print("Media WS: received media and metadata: " + str(data))
                        self.twilio_call = TwilioCall(data["start"]["accountSid"], data["start"]["callSid"])

                    if data['event'] == "media":
                        self.lex_streaming_client.stream_to_lex(data["media"]["payload"])
                    if data['event'] == "closed":
                        log("Closed Message received", message)
                        break

        except Exception as e:
            self.logger.exception(e)

    def pause_listening(self):
        self.listen_switch.set()

    def reset(self):
        self.logger.info("recreating VAD lex client")
        self.lex_streaming_client = VoiceAndSilenceDetectingLexClient(self.user_id, [self], [self])
        self.listen_switch.clear()

    def voice_detected(self):
        self.logger.info("voice detected in input stream passed to fancy lex client")
        self.pause_playback()

    def silence_detected(self, **kwargs):
        self.logger.info("silence detected in input stream passed. stop listening for additional data from client, process the collected data and send the result to play back")
        for key, value in kwargs.items():
            self.logger.info("{0} = {1}".format(key, value))
        self.listen_switch.set()
        self.process()
        self.send_data_to_client(kwargs.get("lex_response"))
        self.listen_switch.clear()
        self.reset()

    def pause_playback(self):
        self.logger.info("if something is being played back on connection, now is the time to stop it")
        # stop playback processing here

    def process(self):
        self.logger.info("processing data listened so far")

    # create a new TwiML,
    # update the call.
    def send_data_to_client(self, lex_response):
        self.logger.info("sending data to client {0}".format(lex_response))
        response = VoiceResponse()
        response.say(lex_response.get("Message"))

        dialog_state = lex_response.get("DialogState")
        intent_name = lex_response.get("IntentName")

        if intent_name is not None and intent_name == "GoodbyeIntent" and dialog_state == "Fulfilled" :
            # hang up the call after this
            response.hangup()
        else:
            response.pause(40)

        self.logger.info("response is {0}".format(response))
        self.twilio_call.persist(response.to_xml())
        self.twilio_call.update()

if __name__ == '__main__':
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler

    server = pywsgi.WSGIServer(('', HTTP_SERVER_PORT), app, handler_class=WebSocketHandler)
    print("Server listening on: http://localhost:" + str(HTTP_SERVER_PORT))
    server.serve_forever()

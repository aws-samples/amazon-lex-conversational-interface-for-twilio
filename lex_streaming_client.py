"""
This module contains functionality to stream data to Amazon Lex

The LexClientStreaming runs in its own thread. Class calls post content on Lex
and uses chunked upload of data by passing in data iterator when creating the POST
connection to server. Clients of this class can keep adding to data by calling
add_to_stream(). Once done, clients need to call stop(), at which point the
iterator finally finishes. Iterator checks if there is more data to send or not
by periodically looking at the data input queue.

Todo:
    * TBD
"""

import datetime
import hashlib
import hmac
import logging
import threading
import time
import requests
import os

class LexClientStreaming:
    AUDIO_CONTENT_TYPE = 'audio/lpcm; sample-rate=8000; sample-size-bits=16; channel-count=1; is-big-endian=false'
    TEXT_CONTENT_TYPE = 'text/plain; charset=utf-8'

    lex_config = {
        "AccessKeyId": os.environ.get('ACCESS_KEY_ID'),
        "SecretAccessKey": os.environ.get('SECRET_ACCESS_KEY'),
        "Region": os.environ.get('AWS_REGION'),
        "BotName": os.environ.get('LEX_BOT_NAME'),
        "BotAlias": os.environ.get('LEX_BOT_ALIAS')
    }

    def __init__(self, user_id, content_type=AUDIO_CONTENT_TYPE, stage="lex"):
        self.logger = logging.getLogger(__name__)
        self.region = self.lex_config["Region"]
        self.access_key = self.lex_config["AccessKeyId"]
        self.secret_key = self.lex_config["SecretAccessKey"]
        self.bot_name = self.lex_config["BotName"]
        self.bot_alias = self.lex_config["BotAlias"]
        self.host_name = "runtime.lex.{0}.amazonaws.com".format(self.region)
        self.endpoint = "https://runtime.lex.{0}.amazonaws.com".format(self.region)
        self.service = "lex"
        self.data = []
        self.data_index = 0
        self.close_stream = False
        self.user_id = user_id
        self.lex_user_id = "{0}_{1}".format(stage, user_id)
        self.content_type = content_type
        self.response = None
        self.crashed = False
        self.request_thread = None

    # add data if stream is not closed. no-op otherwise
    def add_to_stream(self, data):

        # start a connection first time we see that data is added to stream
        if self.request_thread is None:
            self.request_thread = threading.Thread(target=self.run)
            self.request_thread.start()

        # python operation to append data to new list seems thread safe
        # http://effbot.org/pyfaq/what-kinds-of-global-value-mutation-are-thread-safe.htm
        if self.close_stream is False:
            self.data.append(data)

    def is_alive(self):
        return self.request_thread is not None and self.request_thread.is_alive()

    # stop the stream and wait for this thread to finish.
    def stop(self):
        self.logger.debug("closing lex streaming client")
        self.close_stream = True
        if self.request_thread is not None:
            self.logger.debug("waiting for lex connection thread to stop")
            self.request_thread.join()
            self.logger.debug("lex connection thread stopped")

    # check (every X milliseconds) and return new chunk if there is data added to stream
    def stream_iterator(self):
        while not self.close_stream:
            if self.data_index < len(self.data):
                if self.content_type == LexClientStreaming.TEXT_CONTENT_TYPE:
                    yield str.encode(self.data[self.data_index])
                else:
                    yield self.data[self.data_index]

                self.data_index = self.data_index + 1
            else:
                # sleeping for 100 ms to check if the stream is closed or not
                time.sleep(0.1)

        # check if anything is left to send as loop could have stopped if stream got closed and chunks have not yet been
        # sent.

        while self.data_index < len(self.data):
            if self.content_type == LexClientStreaming.TEXT_CONTENT_TYPE:
                yield str.encode(self.data[self.data_index])
            else:
                yield self.data[self.data_index]
            self.data_index = self.data_index + 1

    def is_crashed(self):
        return self.crashed

    def run(self):
        try:
            self.__run()
        except Exception as e:
            self.logger.exception(e)
            self.crashed = True

    def __run(self):
        headers = {}
        user_id = self.lex_user_id
        content_type = self.content_type
        data_type = "content"
        payload_hash = "UNSIGNED-PAYLOAD"
        headers['x-amz-content-sha256'] = "UNSIGNED-PAYLOAD"

        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')  # '20170714T010101Z'
        date_stamp = t.strftime('%Y%m%d')  # Date w/o time, used in credential scope '20170714'

        # ************* TASK 1: CREATE A CANONICAL REQUEST *************
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html

        # Step 1 is to define the verb (GET, POST, etc.)
        verb = "POST"

        # Step 2: Create canonical URI--the part of the URI from domain to query
        # string (use '/' if no path)
        canonical_uri = "/bot/{0}/alias/{1}/user/{2}/{3}".format(self.bot_name, self.bot_alias, user_id, data_type)

        # Step 3: Create the canonical query string. In this example, request
        # parameters are passed in the body of the request and the query string
        # is blank.
        canonical_query_string = ""

        # Step 4: Create the canonical headers. Header names must be trimmed
        # and lowercase, and sorted in code point order from low to high.
        # Note that there is a trailing \n.
        canonical_headers = 'content-type:' + content_type + '\n' + 'host:' + self.host_name + '\n' + 'x-amz-date:' + amz_date + '\n'

        # Step 5: Create the list of signed headers. This lists the headers
        # in the canonical_headers list, delimited with ";" and in alpha order.
        # Note: The request can include any headers; canonical_headers and
        # signed_headers include those that you want to be included in the
        # hash of the request. "Host" and "x-amz-date" are always required.
        # For Lex, content-type and x-amz-target are also required.
        signed_headers = 'content-type;host;x-amz-date'

        # Step 6: Create payload hash. In this example, the payload (body of
        # the request) contains the request parameters.

        # overwrite payload hash if it is post text call
        # if data_type == "text":
        #    payload_hash = hashlib.sha256(request_parameters.encode('utf-8')).hexdigest()

        # Step 7: Combine elements to create create canonical request
        canonical_request = verb + '\n' + canonical_uri + '\n' + canonical_query_string + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

        # ************* TASK 2: CREATE THE STRING TO SIGN *************
        # Match the algorithm to the hashing algorithm you use, either SHA-1 or
        # SHA-256 (recommended)
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = date_stamp + '/' + self.region + '/' + self.service + '/' + 'aws4_request'
        string_to_sign = algorithm + '\n' + amz_date + '\n' + credential_scope + '\n' + hashlib.sha256(
            canonical_request.encode('utf-8')).hexdigest()

        # ************* TASK 3: CALCULATE THE SIGNATURE *************
        # Create the signing key using the function defined above.
        signing_key = self.__get_signature_key(self.secret_key, date_stamp, self.region, self.service)

        # Sign the string_to_sign using the signing_key
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
        # Put the signature information in a header named Authorization.
        authorization_header = algorithm + ' ' \
                               + 'Credential=' + self.access_key + '/' + credential_scope + ', ' \
                               + 'SignedHeaders=' + signed_headers + ', ' \
                               + 'Signature=' + signature

        # For Lex, the request can include any headers, but MUST include "host", "x-amz-date",
        # "x-amz-target", "content-type", and "Authorization". Except for the authorization
        # header, the headers must be included in the canonical_headers and signed_headers values, as
        # noted earlier. Order here is not significant.
        # Python note: The 'host' header is added automatically by the Python 'requests' library.
        headers['Content-Type'] = content_type
        headers['X-Amz-Date'] = amz_date
        headers['Authorization'] = authorization_header

        # ************* SEND THE REQUEST *************
        self.logger.debug("Calling Lex to stream data, endpoint: %s", self.endpoint)
        self.response = requests.post(self.endpoint + canonical_uri, data=self.stream_iterator(), headers=headers)
        self.logger.info("Lex response headers %s ", self.response.headers)

    # Key derivation functions.
    # See: http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
    @staticmethod
    def __sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def __get_signature_key(self, key, date_stamp, region_name, service_name):

        k_date = self.__sign(('AWS4' + key).encode('utf-8'), date_stamp)
        k_region = self.__sign(k_date, region_name)
        k_service = self.__sign(k_region, service_name)
        k_signing = self.__sign(k_service, 'aws4_request')
        return k_signing

    def get_response(self):
        
        if self.response is None:
            raise Exception("Cannot normalize response as there is no response from lex yet. check if add_to_stream() has been called.")

        if self.response.status_code != 200:
            raise Exception("Cannot normalize response as call to Lex did not end with status code 200")
        
        return {"DialogState":self.response.headers.get("x-amz-lex-dialog-state"),
                "Message":self.response.headers.get("x-amz-lex-message"),
                "Utterance":self.response.headers.get("x-amz-lex-input-transcript"),
                "LexRequestId":self.response.headers.get("x-amzn-RequestId"),
                "IntentName": self.response.headers.get("x-amz-lex-intent-name")}

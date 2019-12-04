import audioop
import base64
import logging
import time
import threading
from datetime import datetime
from lex_streaming_client import LexClientStreaming

class VoiceAndSilenceDetectingLexClient:
    vad_sd_config = {
        "VoiceThreshold": 500,
        "SilenceDurationTimeInSecs": 2,
        "TwilioRate": 8000,
        "LexRate": 16000,
        "Width": 2,
        "Channels": 1
    }

    def __init__(self, user_id, voice_detected_call_backs=[], silence_detected_call_backs=[]):
        self.logger = logging.getLogger(__name__)
        self.voice_threshold = self.vad_sd_config["VoiceThreshold"]
        self.silence_duration_time = self.vad_sd_config["SilenceDurationTimeInSecs"]
        self.twilio_rate = self.vad_sd_config["TwilioRate"]
        self.lex_rate = self.vad_sd_config["LexRate"]
        self.width = self.vad_sd_config["Width"]
        self.channels = self.vad_sd_config["Channels"]


        self.lex_client = LexClientStreaming(user_id)
        self.rms_graph = []
        self.rms_values = []
        self.last_detected_voice_time = None

        self.voice_detected_call_backs = voice_detected_call_backs
        self.silence_detected_call_backs = silence_detected_call_backs

        self.stop_data_processing = threading.Event()
        self.logger.info("VoiceAndSilenceDetectingLexClient configured with voice threshold {0}, silence duration {1}, lex user id {2}"
                         .format(self.voice_threshold,
                                 self.silence_duration_time,
                                 user_id))


    def stream_to_lex(self, base_64_encoded_data):
        if self.stop_data_processing.is_set():
            self.logger.warn("discarding the passed in data, as underlying lex stream has been stopped")
            return

        data = self.__decode_data(base_64_encoded_data)

        raw_audio_data = audioop.ulaw2lin(data, self.width)
        #raw_audio_data, state = audioop.lin2adpcm(raw_audio_data, self.width, None)

        # raw_audio_data, state = audioop.ratecv(raw_audio_data,
        #                                        self.width,
        #                                        self.channels,
        #                                        self.twilio_rate,
        #                                        self.lex_rate,
        #                                        None)

        rms = audioop.rms(raw_audio_data, self.width)

        #self.logger.info("RMS value is {0}".format(rms))
        self.rms_values.append(rms)
        if rms > self.voice_threshold:
            #self.logger.debug("voice detected in input data")
            self.rms_graph.append("^")

            if self.last_detected_voice_time is None:
                # voice detected for first time
                self.logger.debug("voice detected for first time")
                self.voice_detected()
            self.last_detected_voice_time = datetime.now()
            self.lex_client.add_to_stream(raw_audio_data)
        else:
            self.rms_graph.append(".")
            #self.logger.debug("silence detected in input data")

            if self.last_detected_voice_time:
                # check if elapsed time is greater than configured time for silence
                self.lex_client.add_to_stream(raw_audio_data)

                silence_time = (datetime.now() - self.last_detected_voice_time).total_seconds()
                if silence_time >= self.silence_duration_time:
                    self.logger.debug("elapsed time {0} seconds since last detected voice time {1} is higher than configured time for silence {2} seconds. closing connection to lex."
                                      .format(silence_time,
                                              self.last_detected_voice_time,
                                              self.silence_duration_time))

                    # stop lex client now
                    self.lex_client.stop()
                    self.stop_data_processing.set()
                    self.logger.info("Voice activity graph {0}".format("".join(self.rms_graph)))
                    self.logger.info("RMS values {0}".format(self.rms_values))
                    self.silence_detected()
            #else:
            #   self.logger.debug("voice has not been detected even once. not starting the silence detection counter")

    def __decode_data(self, data):
        return base64.b64decode(data)

    def voice_detected(self):
        self.logger.info("invoking voice detected callbacks")
        for voice_detected_call_back in self.voice_detected_call_backs:
            self.logger.info("invoking voice detected callback {0}".format(voice_detected_call_back))
            voice_detected_call_back.voice_detected()

    def silence_detected(self):

        self.logger.info("invoking silence detected callbacks with lex data ")
        lex_response = self.lex_client.get_response()
        self.logger.info("lex response is {0}".format(lex_response))

        for silence_detected_call_back in self.silence_detected_call_backs:
            self.logger.info("invoking silence detected callback {0}".format(silence_detected_call_back))
            silence_detected_call_back.silence_detected(lex_response = lex_response)

if __name__ == '__main__':
    data = "/v7+/v7+/n7+/v5+fn7+fn7+/v5+/n7+/v7+/v5+fn7+/H5+/vx+/vz+fv78fnz+/Hx+/P5+/P56fvp+evz6fHz6/nr++np6+P54/Ph6fPR+dvr2dnj2/HL+8Hpy9vJyeO/+b/zydnT2+nh++n5+/P5+fnz+/n56/Pp6evb6dn70fnT89Hx0+PZ4ePj6dnr2+nR+9H50+vR6ePz6enr8+g=="
    lex_client = VoiceAndSilenceDetectingLexClient()
    lex_client.stream_to_lex(data)
    time.sleep(2)
    lex_client.stream_to_lex("/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////w==")


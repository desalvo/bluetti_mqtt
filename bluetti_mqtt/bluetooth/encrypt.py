import logging
import bluetti_crypt
from ctypes import *

class bleEncrypt:
    def __init__(self):
        # self.cryptoClient = bluetti_crypt.BluettiCrypt()
        # bluetti_crypt.test_module()
        pass
    def start(self):
        self.cryptoClient = bluetti_crypt.BluettiCrypt()
        # self.cryptoClient.test_api()
        pass
    def encrypt_link(self, data: bytearray):
        logging.info(' encrypt link data: ' + data.hex())
        # message = str(1024)
        message, ret = self.cryptoClient.ble_crypt_link_handler(bytes(data))
        logging.info(' ble crypt link result: ' + str(ret) + ' message: ' + message.hex())
        return ret, message

    def send_message(self, data: bytearray):
        logging.info('send message: ' + data.hex())
        # message = bytes(1024)
        message = self.cryptoClient.encrypt_data(bytes(data))
        return len(message), message

    def message_handle(self, data: bytearray):
        # logging.info('receive message: ' + data.hex())
        # message = bytes(1024)
        message = self.cryptoClient.decrypt_data(bytes(data))
        logging.info('receive message: ' + message.hex())
        # logging.info('message_handle length: ' + str(length) + " msg: " + message[:length].hex())
        return len(message), message

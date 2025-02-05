from utils.Connector import *

class ServiceDesk_Connector(Connector):
    def __init__(self, logger):
        super().__init__(logger, token_key='service_desk_tokens')
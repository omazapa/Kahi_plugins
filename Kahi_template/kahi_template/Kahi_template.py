from kahi.KahiBase import KahiBase


class Kahi_template(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

    def run(self):
        # entry point for the execution of the plugin
        # magic happens here...
        pass

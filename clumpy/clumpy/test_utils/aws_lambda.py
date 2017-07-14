import datetime


class FakeContext():
    def __init__(self, seconds=300):
        self._time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)

        self.function_name = "function_name"
        self.function_version = "function_version"
        self.invoked_function_arn = "invoked_function_arn"
        self.memory_limit_in_mb = 192
        self.aws_request_id = "aws_request_id"
        self.log_group_name = "log_group_name"
        self.log_stream_name = "log_stream_name"
        self.identity = "identity"
        self.client_context = "client_context"

    def get_remaining_time_in_millis(self):
        return (self._time - datetime.datetime.now()).total_seconds() * 1000

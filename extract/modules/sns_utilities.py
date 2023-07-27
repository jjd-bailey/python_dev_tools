import boto3
from typing import Union



class SnsFailure:
    def __init__(self, topic: str, job: str, definitions: str):
        self.topic = topic
        self.job = job
        self.definitions = definitions

    def _sns_post_failure(
            self,
            crtitical: bool,
            error: str,
            more_info: Union[str, None] = None
        ):
        severity = 'CRITICAL' if crtitical else 'MEDIUM'
        subject = f"{severity} - GLUE JOB FAILURE"
        message = [
            "FAILURE RESULTING IN {outcome} JOB FAILURE\n\n",
            f"Job Name: {self.job}\n",
            f"Table Definition: {self.definitions}\n\n"
            "--Error--\n{error}"
        ]
        if more_info is not None:
            message = message + [
                f"\n\n--More Info--\n{more_info}"
            ]
        message = ''.join(message)
        message = message.format(
            severity = severity.upper(),
            outcome = 'COMPLETE' if severity else 'PARTIAL',
            error = error
        )
        sns = boto3.client('sns')
        sns.publish(
            TopicArn = self.topic,
            Subject = subject,
            Message = message
        )

    def post_error_critical(self, error: str, more_info: Union[str, None] = None):
        self._sns_post_failure(
            crtitical = True,
            error = error,
            more_info = more_info
        )

    def post_error_medium(self, error: str, more_info: Union[str, None] = None):
        self._sns_post_failure(
            crtitical = False,
            error = error,
            more_info = more_info
        )

    def post_warning(self, warning: str):
        subject = f"WARNING - GLUE JOB"
        message = [
            "WARNING IN GLUE JOB\n\n",
            f"Job Name: {self.job}\n",
            f"Table Definition: {self.definitions}\n\n"
            f"--Warning--\n{warning}"
        ]
        sns = boto3.client('sns')
        sns.publish(
            TopicArn = self.topic,
            Subject = subject,
            Message = ''.join(message)
        )
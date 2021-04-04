from typing import Optional, Iterable, List

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor


class EmrStepsSensor(EmrStepSensor):

    template_fields = ['job_flow_id', 'step_id', 'target_states', 'failed_states']
    template_ext = ()

    def __init__(
        self,
        *,
        job_flow_id: str,
        step_id: List[str],
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states or ['COMPLETED']
        self.failed_states = failed_states or ['CANCELLED', 'FAILED', 'INTERRUPTED']

    def poke(self, context):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Job flow currently %s', state)

        if state in self.target_states:
            return True

        if state in self.failed_states:
            final_message = 'EMR job failed'
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message += ' ' + failure_message
            raise AirflowException(final_message)

        return False
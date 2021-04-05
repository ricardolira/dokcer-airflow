from typing import Optional, Iterable, List, Dict, Any

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor


class EmrStepsSensor(EmrStepSensor):

    template_fields = ['job_flow_id', 'step_id', 'target_states', 'failed_states']
    template_ext = ()

    def __init__(
        self,
        *,
        job_flow_id: str,
        steps_id: List[str],
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.steps_id = steps_id
        self.target_states = target_states or ['COMPLETED']
        self.failed_states = failed_states or ['CANCELLED', 'FAILED', 'INTERRUPTED']

    def poke(self, context):
        states = []
        responses = self.get_emr_response()
        for response in responses:
            if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.log.info('Bad HTTP response: %s', response)
                return False

            state = self.state_from_response(response)
            self.log.info('Job flow currently %s', state)
            if state in self.failed_states:
                final_message = 'EMR job failed'
                failure_message = self.failure_message_from_response(response)
                if failure_message:
                    final_message += ' ' + failure_message
                raise AirflowException(final_message)
            states.append(state)

        if all([state in self.target_states for state in states]):
            return True
        return False

    def get_emr_response(self) -> List[Dict[str, Any]]:
        """
        Make an API call with boto3 and get details about the steps sent in
        batch to the cluster.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: response
        :rtype: dict[str, Any]
        """
        emr_client = self.get_hook().get_conn()
        steps_ids = ", ".join(self.steps_id)
        self.log.info('Poking steps %s on cluster %s', steps_ids, self.job_flow_id)
        return [
            emr_client.describe_step(ClusterId=self.job_flow_id, StepId=step_id)
            for step_id in self.steps_id
        ]
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.decorators import apply_defaults
from typing import Any


class SmartEmrStepSensor(EmrStepSensor):
    poke_context_fields = (
        'job_flow_id', 'step_id', 'target_states', 'failed_states'
    )  # <- Required

    @apply_defaults
    def __init__(self,  **kwargs: Any):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):  # <- Required
        result = (
            not self.soft_fail
            and super().is_smart_sensor_compatible()
        )
        return result

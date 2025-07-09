from anyio.abc import TaskStatus
from prefect.client.schemas.objects import FlowRun
from prefect.workers.base import BaseJobConfiguration, BaseVariables, BaseWorker, BaseWorkerResult
from pydantic import Field
from pydantic_settings import BaseSettings

CPU_COMMAND = (
    "sbatch --constraint=cpu "
    "--qos={qos} "
    "--time {max_wall_time} "
    "--nodes {nodes} "
    "--account {project} "
    "--job-name {name} "
    "podman-hpc run --rm --entrypoint {entrypoint} {image}:{tag}"
)


class NerscWorkerConfiguration(BaseSettings):
    time_between_queries: int = Field(
        default=30,
        description="Time in seconds between job status queries. "
        "See https://docs.nersc.gov/jobs/best-practices/#limit-your-queries-to-the-batch-system",
        # Note try to use sacct https://docs.nersc.gov/jobs/monitoring/#sacct for monitoring jobs
    )


class NerscJobConfiguration(BaseJobConfiguration):
    image: str = Field(
        description="Docker image to repo",
    )
    tag: str = Field(
        default="latest",
        description="Docker image tag to use for the job.",
    )
    name: str = Field(
        default="prefect-job",
        description="Name of the job to use for the job.",
    )
    project: str = Field(
        default="default",
        description="Project name to use for the job for billing.",
    )
    qos: str = Field(
        default="shared",
        description="Quality of Service to use for the job. "
        "For NERSC, you have debug, regular, preempt, premium, interactive, shared_interactive, shared. "
        "Refer to https://docs.nersc.gov/jobs/policy/#qos-cost-factor-charge-multipliers-and-discounts for details.",
    )
    nodes: int = Field(
        default=1,
        description="Number of nodes to allocate for the job.",
    )
    processes_per_node: int = Field(
        default=1,
        description="Number of processes to allocate per node.",
    )
    memory: int = Field(
        default=1024,
        description="Memory in MB to allocate.",
    )
    max_walltime: int = Field(
        default=3600,
        description="Maximum wall time in seconds.",
    )


class NerscTemplateVariables(BaseVariables):
    pass


class NerscWorkerResult(BaseWorkerResult):
    pass


class NerscWorker(BaseWorker[NerscJobConfiguration, NerscTemplateVariables, NerscWorkerResult]):
    type: str = "slurm"
    job_configuration = NerscJobConfiguration
    job_configuration_variables = NerscTemplateVariables

    _display_name = "nersc"
    _logo_url = "https://static.wikia.nocookie.net/enfuturama/images/d/df/Slurmlogo.png"

    async def run(
        self,
        flow_run: FlowRun,
        configuration: BaseJobConfiguration,
        task_status: TaskStatus | None = None,
    ) -> NerscWorkerResult:
        if task_status is not None:
            task_status.started()

        # Simulate job execution
        await self.run_flow(flow=flow_run)

        return NerscWorkerResult(status_code=0, identifier="fake_identifier")

    async def run_flow(self, flow: FlowRun) -> None:
        pass

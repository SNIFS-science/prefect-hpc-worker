import asyncio

from anyio.abc import TaskStatus
from prefect.client.schemas.objects import FlowRun
from prefect.workers.base import BaseJobConfiguration, BaseVariables, BaseWorker, BaseWorkerResult
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings

CPU_COMMAND = (
    "sbatch --constraint=cpu "
    "--qos={qos} "
    "--time {max_wall_time} "
    "--nodes {nodes} "
    "--account {project} "
    "--job-name {name} "
    "podman-hpc run --rm --entrypoint {entrypoint} {volume_str} {image}:{tag}"
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
    volumes: list[tuple[str, str, str]] = Field(
        default_factory=list,
        description="List of volumes to mount in the format (host_path, container_path, options). "
        "Example: [('/host/path', '/container/path', 'rw')]",
    )

    @property
    @computed_field
    def volume_str(self) -> str:
        return " ".join(
            f"--volume {host_path}:{container_path}:{options}" for host_path, container_path, options in self.volumes
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
        configuration: NerscJobConfiguration,
        task_status: TaskStatus | None = None,
    ) -> NerscWorkerResult:
        if task_status is not None:
            task_status.started()

        return await self.run_flow(flow=flow_run, configuration=configuration)

    async def run_flow(self, flow: FlowRun, configuration: NerscJobConfiguration) -> NerscWorkerResult:
        command = CPU_COMMAND.format(configuration.model_dump())
        self._logger.info(f"Running command: {command}")
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            self._logger.info(f"Command output: {stdout.decode()}")
        if stderr:
            self._logger.error(f"Command error: {stderr.decode()}")

        # Get exit status of the command
        if proc.returncode is None:
            self._logger.error("Command did not complete successfully, return code is None")
            return NerscWorkerResult(status_code=-1, identifier="unknown")
        if proc.returncode != 0:
            self._logger.error(f"Command failed with exit code {proc.returncode}")

        # TODO get the slurm id and truen this in the result
        # TODO: figure out error handling from stdout, stderr
        return NerscWorkerResult(status_code=proc.returncode, identifier=str(proc.pid))

import asyncio
import sys
from typing import TYPE_CHECKING

import prefect
from anyio.abc import TaskStatus
from prefect.client.schemas.objects import FlowRun
from prefect.utilities.slugify import slugify
from prefect.workers.base import BaseJobConfiguration, BaseVariables, BaseWorker, BaseWorkerResult
from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from prefect.client.schemas.objects import Flow, FlowRun
    from prefect.client.schemas.responses import DeploymentResponse


def python_version_minor() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def python_version_micro() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


CPU_COMMAND = (
    "srun --constraint=cpu "
    "--qos={qos} "
    "--time {max_walltime_str} "
    "--nodes {nodes} "
    "--account {project} "
    "--job-name {name} "
    "--mem {memory} "
    "--cpus-per-task {processes_per_node} "
    "--ntasks {tasks} "
    "--output slurm_%A.log "
    "podman-hpc run --rm {volume_str} {env_str} {image} {command}"
)


def get_prefect_image_name(
    prefect_version: str | None = None,
    python_version: str | None = None,
    flavor: str | None = None,
) -> str:
    """
    Get the Prefect image name matching the current Prefect and Python versions.

    Args:
        prefect_version: An optional override for the Prefect version.
        python_version: An optional override for the Python version; must be at the
            minor level e.g. '3.9'.
        flavor: An optional alternative image flavor to build, like 'conda'
    """
    parsed_version = (prefect_version or prefect.__version__).split("+")
    is_prod_build = len(parsed_version) == 1
    prefect_version = parsed_version[0] if is_prod_build else "sha-" + prefect.__version_info__["full-revisionid"][:7]

    python_version = python_version or python_version_minor()

    tag = slugify(
        f"{prefect_version}-python{python_version}" + (f"-{flavor}" if flavor else ""),
        lowercase=False,
        max_length=128,
        # Docker allows these characters for tag names
        regex_pattern=r"[^a-zA-Z0-9_.-]+",
    )

    image = "prefect" if is_prod_build else "prefect-dev"
    return f"prefecthq/{image}:{tag}"


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
    name: str = Field(
        default="prefect-job",
        description="Name of the job to use for the job.",
    )
    project: str = Field(
        default="m112",
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
    tasks: int = Field(
        default=1,
        description="Number of tasks to allocate for the job. "
        "This is the number of processes that will run in parallel.",
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
    entrypoint: str | None = Field(
        default="/bin/bash",
        description="Entrypoint to use for the container",
    )
    command: str | None = Field(
        default=None,
        description="Command to run in the container.",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        title="Environment Variables",
        description="Environment variables to set when starting a flow run.",
    )

    @computed_field
    @property
    def volume_str(self) -> str:
        return " ".join(
            f"--volume {host_path}:{container_path}:{options}" for host_path, container_path, options in self.volumes
        )

    @computed_field
    @property
    def max_walltime_str(self) -> str:
        # Turns a number of seconds into a string formatted as HH:MM:SS
        hours, remainder = divmod(self.max_walltime, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    @computed_field
    @property
    def env_str(self) -> str:
        return " ".join(f"--env {key}={value}" for key, value in self.env.items())

    @field_validator("volumes")
    @classmethod
    def _validate_volumes(cls, volumes: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
        for item in volumes:
            assert len(item) == 3, "Each volume must be a tuple of (host_path, container_path, options)"
            host_path, container_path, options = item
            assert host_path.startswith("/"), "Host path must be an absolute path starting with '/'"
            assert container_path.startswith("/"), "Container path must be an absolute path starting with '/'"
            assert options in ("ro", "rw"), "Options must be either 'ro' or 'rw'"
        return volumes

    def _slugify_container_name(self) -> str:
        """
        Generates a container name to match the configured name, ensuring it is Docker
        compatible.
        """
        # Must match `/?[a-zA-Z0-9][a-zA-Z0-9_.-]+` in the end
        return slugify(
            self.name,
            lowercase=False,
            # Docker does not limit length but URL limits apply eventually so
            # limit the length for safety
            max_length=250,
            # Docker allows these characters for container names
            regex_pattern=r"[^a-zA-Z0-9_.-]+",
        ).lstrip(
            # Docker does not allow leading underscore, dash, or period
            "_-."
        )

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: "DeploymentResponse | None" = None,
        flow: "Flow | None" = None,
    ):
        """
        Prepares the agent for a flow run by setting the image, labels, and name
        attributes.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow)
        if deployment is not None:
            self.entrypoint = deployment.entrypoint
        self.image = self.image or get_prefect_image_name()
        self.name = self._slugify_container_name()


class NerscWorkerResult(BaseWorkerResult):
    pass


class NerscWorker(BaseWorker[NerscJobConfiguration, BaseVariables, NerscWorkerResult]):
    type: str = "nersc"
    job_configuration = NerscJobConfiguration

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
        try:
            configuration.prepare_for_flow_run(flow)
            values = configuration.model_dump()
            self._logger.info(f"Running flow with configuration: {configuration.model_dump_json(indent=2)}")
            self._logger.info(f"Formatting CPU command: {CPU_COMMAND}")
            command = CPU_COMMAND.format(**values)
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
        except Exception as e:
            self._logger.error(f"Error running flow: {e}")
            return NerscWorkerResult(status_code=-1, identifier="unknown")

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from invoke import task

load_dotenv()


@task
def create_pubsub(
    ctx: Any,
    topic: str,
    create_subscription: bool = False,
    message_retention_duration: int = 60 * 60 * 24,
    retain_acked_messages: bool = False,
) -> None:
    """Create a Pub/Sub topic and optional same-name subscription."""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import pubsub_v1
    from google.protobuf.duration_pb2 import Duration

    project_id = os.environ["PROJECT_ID"]
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic)
    subscription_path = subscriber.subscription_path(project_id, topic)

    try:
        publisher.create_topic(request={"name": topic_path})
    except AlreadyExists:
        pass

    if create_subscription:
        request = {
            "name": subscription_path,
            "topic": topic_path,
            "enable_message_ordering": True,
        }
        if message_retention_duration:
            retention = Duration()
            retention.FromSeconds(message_retention_duration)
            request["message_retention_duration"] = retention
        if retain_acked_messages:
            request["retain_acked_messages"] = True
        with subscriber:
            try:
                subscriber.create_subscription(request=request)
            except AlreadyExists:
                pass


def get_docker_secrets() -> str:
    """Build Docker args for deploy-time environment values."""
    build_args = [
        f'{secret}="{os.environ[secret]}"' for secret in ("PROJECT_ID", "SENTRY_DSN")
    ]
    return " ".join([f"--build-arg {build_arg}" for build_arg in build_args])


def get_container_name(
    hostname: str = "asia.gcr.io",
    image: str = "asyncio-quant-tick",
    tag: str | None = None,
) -> str:
    """Build the GCR image name."""
    project_id = os.environ["PROJECT_ID"]
    container_name = f"{hostname}/{project_id}/{image}"
    if tag:
        container_name += f":{tag}"
    return container_name


@task
def build_container(
    ctx: Any, hostname: str = "asia.gcr.io", image: str = "asyncio-quant-tick"
) -> None:
    """Build the deploy container from the latest wheel."""
    wheel = build_quant_tick(ctx)
    requirements = Path("requirements.txt")
    ctx.run(
        "uv export "
        "--format requirements.txt "
        "--extra gcp "
        "--group deploy "
        "--no-dev "
        "--no-header "
        "--no-annotate "
        "--no-editable "
        "--no-hashes "
        "--no-emit-project "
        f"--output-file {requirements}"
    )
    name = get_container_name(hostname, image)
    build_args = {"WHEEL": wheel}
    build_args = " ".join(
        [f'--build-arg {key}="{value}"' for key, value in build_args.items()]
    )
    docker_secrets = get_docker_secrets()
    cmd = f"docker build {build_args} {docker_secrets} --file Dockerfile --tag {name} ."
    ctx.run(cmd)


def build_quant_tick(ctx: Any) -> str:
    """Build the wheel and return its filename."""
    dist_dir = Path("dist")
    if dist_dir.exists():
        for wheel in dist_dir.glob("asyncio_quant_tick-*.whl"):
            wheel.unlink()
    ctx.run("uv build --wheel")
    wheels = sorted(dist_dir.glob("asyncio_quant_tick-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one built wheel, found {len(wheels)}")
    return wheels[0].name


@task
def push_container(
    ctx: Any, hostname: str = "asia.gcr.io", image: str = "asyncio-quant-tick"
) -> None:
    """Push the deploy container image."""
    name = get_container_name(hostname, image)
    ctx.run(f"docker push {name}")


@task
def deploy_container(
    ctx: Any,
    name: str = "asyncio-quant-tick",
    container_name: str | None = None,
    machine_type: str = "e2-micro",
    zone: str = "asia-northeast1-a",
) -> None:
    """Create the Compute Engine container instance."""
    container_name = container_name or get_container_name(tag="latest")
    service_account = os.environ["SERVICE_ACCOUNT"]
    # A best practice is to set the full cloud-platform access scope on the instance,
    # then securely limit the service account's API access with IAM roles.
    # https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam
    scopes = "cloud-platform"
    cmd = f"""
        gcloud compute instances create-with-container {name} \
            --machine-type {machine_type} \
            --zone {zone} \
            --container-image {container_name} \
            --service-account {service_account} \
            --scopes {scopes}
    """
    ctx.run(cmd)


@task
def update_container(
    ctx: Any, hostname: str = "asia.gcr.io", name: str = "asyncio-quant-tick"
) -> None:
    """Build, push, and restart the container instance."""
    build_container(ctx, hostname=hostname, image=name)
    push_container(ctx, hostname=hostname, image=name)
    ctx.run(f"gcloud compute instances reset {name}")

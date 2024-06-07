from functools import cached_property

from lightkube.core.client import Client
from lightkube.core.resource import NamespacedResource
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Node, Pod, Service
from lightkube.types import PatchType

from core.cluster import ClusterState
from core.workload import WorkloadBase


class K8sManager:
    """Object for managing K8s patches."""

    def __init__(self, state: ClusterState, workload: WorkloadBase):
        self.state = state
        self.workload = workload

    @cached_property
    def client(self) -> Client:
        return Client()  # pyright: ignore[reportArgumentType]

    @property
    def pod(self) -> NamespacedResource:
        return self.client.get(
            Pod,
            name=self.state.unit_broker.unit.name.replace("/", "-"),
            namespace=self.state.model.name,
        )

    @property
    def node(self) -> NamespacedResource:
        return self.client.get(
            Node,
            name=self.pod.spec.nodeName,
            namespace=self.state.model.name,
        )

    @property
    def service(self) -> NamespacedResource:
        return self.client.get(
            Service,
            name=f"{self.state.cluster.app.name}-external",
            namespace=self.state.model.name,
        )

    @property
    def node_port(self) -> int:
        if not self.service or not self.service.spec.type == "NodePort":
            return -1

        for svc_port in self.service.spec.ports:
            if svc_port.port == 9093:
                return svc_port.nodePort

        raise Exception("NodePort not found.")

    # TODO: fix this thing
    @property
    def node_ip_ports(self) -> list[str]:
        ip_types = ["ExternalIP", "InternalIP", "Hostname"]
        node_ip = ""

        for addresses in self.node.status.addresses:
            if addresses.type in ip_types:
                node_ip = addresses.address

        return f"{node_ip}:{self.node_port}"

    @property
    def nodeport_service(self) -> None:
        service = Service(
            metadata=ObjectMeta(
                name=self.state.cluster.app.name,
                namespace=self.state.model.name,
            ),
            spec=ServiceSpec(
                ports=[
                    ServicePort(
                        name=f"{self.state.cluster.app.name}-port",
                        port=9093,
                        targetPort=9093,
                        protocol="TCP",
                    )
                ],
                type="NodePort",
                selector={
                    "app.kubernetes.io/name": self.state.unit_broker.unit.name.replace("/", "-")
                },
            ),
        )

        self.client.patch(
            res=Service,
            obj=service,
            name=service.metadata.name,
            namespace=service.metadata.namespace,
            force=True,
            field_manager=self.state.cluster.app.name,
            patch_type=PatchType.MERGE,
        )

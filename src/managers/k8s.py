import logging
from functools import cached_property

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.core.resource import NamespacedResource
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Node, Pod, Service

from core.cluster import ClusterState

logger = logging.getLogger(__name__)

class K8sManager:
    """Object for managing K8s patches."""
    PORT_MINIMUM = 30000
    KAFKA_PORT_OFFSET = 1000

    def __init__(self, state: ClusterState):
        self.state = state

    @cached_property
    def client(self) -> Client:
        return Client(field_manager=self.state.cluster.app.name)  # pyright: ignore[reportArgumentType]

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
    def node_ip(self) -> str:
        for addresses in self.node.status.addresses:
            if addresses.type in ["ExternalIP", "InternalIP", "Hostname"]:
                return addresses.address

        return ""

    @property
    def service(self) -> NamespacedResource | None:
        try:
            return self.client.get(
                Service,
                name=self.state.unit_broker.unit.name.replace("/", "-"),
                namespace=self.state.model.name,
            )
        except ApiError as e:  # in case the service hasn't been created yet
            logger.warning(e)
            return

    @property
    def node_port(self) -> int:
        return self.PORT_MINIMUM + self.KAFKA_PORT_OFFSET + self.state.unit_broker.unit_id

    def patch_external_service(self, svc_port: int) -> None:
        service = Service(
            metadata=ObjectMeta(
                name=self.state.unit_broker.unit.name.replace("/", "-"),
                namespace=self.state.model.name,
            ),
            spec=ServiceSpec(
                externalTrafficPolicy="Local",
                type="NodePort",
                selector={
                    "statefulset.kubernetes.io/pod-name": self.state.unit_broker.unit.name.replace("/", "-")
                },
                ports=[
                    ServicePort(
                        protocol="TCP",
                        port=svc_port,
                        targetPort=svc_port,
                        nodePort=self.node_port,
                        name=f"{self.state.cluster.app.name}-port",
                    ),
                ],
            ),
        )

        self.client.apply(service)

#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Kubernetes resources."""

import logging
from functools import cached_property

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Node, Pod, Service

from core.cluster import ClusterState

logger = logging.getLogger(__name__)

# default logging from lightkube httpx requests is very noisy
logging.getLogger("lightkube").disabled = True
logging.getLogger("lightkube.core.client").disabled = True
logging.getLogger("httpx").disabled = True


class K8sManager:
    """Manager for handling Kafka Kubernetes resources."""

    PORT_MINIMUM = 30000
    KAFKA_PORT_OFFSET = 1000  # in future, we may have more than one service, add offsets

    def __init__(self, state: ClusterState):
        self.state = state

    @cached_property
    def client(self) -> Client:
        """The Lightkube client."""
        return Client(  # pyright: ignore[reportArgumentType]
            field_manager=self.state.cluster.app.name,
            namespace=self.state.model.name,
        )

    @property
    def pod(self) -> Pod:
        """The Pod of the current running unit."""
        return self.client.get(
            res=Pod,
            name=self.state.unit_broker.unit.name.replace("/", "-"),
        )

    @property
    def node(self) -> Node:
        """The Node the current running unit is on."""
        if not self.pod.spec or not self.pod.spec.nodeName:
            raise Exception("Could not find podSpec or nodeName")

        return self.client.get(
            Node,
            name=self.pod.spec.nodeName,
        )

    @property
    def node_ip(self) -> str:
        """The ip address of the Node the current running unit is on."""
        # all these redundant checks are because Lightkube's typing is awful
        if not self.node.status or not self.node.status.addresses:
            raise Exception(f"No status found for {self.node}")

        for addresses in self.node.status.addresses:
            if addresses.type in ["ExternalIP", "InternalIP", "Hostname"]:
                return addresses.address

        return ""

    @property
    def service_name(self) -> str:
        """The name for the NodePort service."""
        return self.state.unit_broker.unit.name.replace("/", "-")

    @property
    def node_port(self) -> int:
        """The nodePort to assign for the current running unit.

        Kafka listeners need to have unique ports, and NodePorts must be between 30000 and 32767.
        It is also helpful for ports to be unique, so as to support multiple brokers on the same node.
        NodePorts also must be between 30000 and 32767.
        """
        return self.PORT_MINIMUM + self.KAFKA_PORT_OFFSET + self.state.unit_broker.unit_id

    def create_nodeport_service(self, svc_port: int) -> None:
        """Creates a NodePort service for external access to the current running unit.

        In order to discover all Kafka brokers, a client application must know the location of at least 1
        active broker, `bootstrap-server`. From there, the broker returns the `advertised.listeners`
        to the client application, here specified as <NODE-IP>:<NODE-PORT>.

        K8s-external requests hit <NODE-IP>:<NODE-PORT>, and are redirected to the corresponding
        statefulset.kubernetes.io/pod-name from the selector, and port from `svc_port`.

        If a pod was rescheduled to a new node, the node-ip defined in the `advertised.listeners`
        will be updated during the normal charm `config-changed` reconciliation.

        Args:
            svc_port: the port for the client service, as defined in the `listeners` server property
        """
        service = Service(
            metadata=ObjectMeta(
                name=self.service_name,
                namespace=self.state.model.name,
            ),
            spec=ServiceSpec(
                externalTrafficPolicy="Local",
                type="NodePort",
                selector={"statefulset.kubernetes.io/pod-name": self.service_name},
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

    @property
    def service(self) -> Service | None:
        """The external NodePort Service created by the charm."""
        try:
            return self.client.get(
                res=Service,
                name=self.service_name,
            )
        except ApiError as e:
            # don't worry about defining a service during cluster init
            # as it doesn't exist yet to `kubectl get`
            logger.warning(e)
            return

    def delete_external_service(self) -> None:
        """Deletes the NodePort service for the current running unit."""
        self.client.delete(res=Service, name=self.service_name, namespace=self.state.model.name)

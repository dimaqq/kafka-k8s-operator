#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Kubernetes resources for a single Kafka pod."""

import logging
from functools import cached_property
from typing import TYPE_CHECKING

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.core_v1 import Node, Pod, Service

from literals import SECURITY_PROTOCOL_PORTS, AuthMechanism

logger = logging.getLogger(__name__)

# default logging from lightkube httpx requests is very noisy
logging.getLogger("lightkube").disabled = True
logging.getLogger("lightkube.core.client").disabled = True
logging.getLogger("httpx").disabled = True

if TYPE_CHECKING:
    from core.models import KafkaBroker


class K8sManager:
    """Manager for handling Kafka Kubernetes resources for a single Kafka pod."""

    def __init__(
        self,
        broker: "KafkaBroker",
        security_protocol: AuthMechanism = "SASL_PLAINTEXT",
    ):
        self.broker = broker
        self._security_protocol: AuthMechanism = security_protocol

    @cached_property
    def client(self) -> Client:
        """The Lightkube client."""
        return Client(  # pyright: ignore[reportArgumentType]
            field_manager=self.broker.unit.name,
            namespace=self.broker.unit._backend.model_name,
        )

    @property
    def security_protocol(self) -> AuthMechanism:
        """The proposed security protocol for the service."""
        return self._security_protocol

    @security_protocol.setter
    def security_protocol(self, value: AuthMechanism) -> None:
        """Internal security_protocol setter."""
        self._security_protocol = value

    @property
    def pod_name(self) -> str:
        """The name of the K8s pod."""
        return self.broker.unit.name.replace("/", "-")

    @cached_property
    def pod(self) -> Pod:
        """The Pod of the current running unit."""
        return self.client.get(
            res=Pod,
            name=self.pod_name,
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
    def service_name(self) -> str:
        """The service name for a given auth mechanism."""
        return f"{self.pod_name}-{self.security_protocol.lower().replace('_','-')}"

    @property
    def bootstrap_service_name(self) -> str:
        """The service name for a given auth mechanism."""
        return f"{self.pod_name}-bootstrap"

    @property
    def bootstrap_node_port(self) -> int:
        """The port number for the bootstrap-server NodePort service."""
        return 31000 + self.broker.unit_id

    @property
    def external_address(self) -> str:
        """The full address for the pod via nodePort service."""
        return f"{self.node_ip}:{self.node_port}"

    @cached_property
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

    @property
    def node_port(self) -> int:
        """The port number of the Node the current running unit is on."""
        # all these redundant checks are because Lightkube's typing is awful
        if not self.service:
            return -1

        if not self.service.spec or not self.service.spec.ports:
            raise Exception("Could not find Service spec or ports")

        return self.service.spec.ports[0].nodePort

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

    def create_bootstrap_service(self) -> None:
        """Creates a NodePort service for bootstrap-servers during initial client connection.

        In order to discover all Kafka brokers, a client application must know the location of at least 1
        active broker, `bootstrap-server`. From there, the broker returns the `advertised.listeners`
        to the client application, here specified as <NODE-IP>:<NODE-PORT>.

        K8s-external requests hit <NODE-IP>:<NODE-PORT>, and are redirected to the corresponding
        statefulset.kubernetes.io/pod-name from the selector, and port matching the auth mechanism.

        If a pod was rescheduled to a new node, the node-ip defined in the `advertised.listeners`
        will be updated during the normal charm `config-changed` reconciliation.
        """
        if not self.pod.metadata:
            raise Exception("Could not find pod metadata")

        svc_port = SECURITY_PROTOCOL_PORTS[self.security_protocol].client

        service = Service(
            metadata=ObjectMeta(
                name=self.bootstrap_service_name,
                namespace=self.broker.unit._backend.model_name,
                ownerReferences=[
                    OwnerReference(
                        apiVersion=self.pod.apiVersion,
                        kind=self.pod.kind,
                        name=self.pod_name,
                        uid=self.pod.metadata.uid,
                        blockOwnerDeletion=False,
                    )
                ],
            ),
            spec=ServiceSpec(
                externalTrafficPolicy="Local",
                type="NodePort",
                selector={"statefulset.kubernetes.io/pod-name": self.pod_name},
                ports=[
                    ServicePort(
                        protocol="TCP",
                        port=svc_port,
                        targetPort=svc_port,
                        name=f"{self.bootstrap_service_name}-port",
                        nodePort=self.bootstrap_node_port,
                    ),
                ],
            ),
        )

        self.client.apply(service)  # pyright: ignore[reportAttributeAccessIssue]

    def create_nodeport_service(self) -> None:
        """Creates a NodePort service for external access to the current running unit.

        These services will be returned by clients connecting to bootstrap-servers, in the advertised.listeners.
        """
        if not self.pod.metadata:
            raise Exception("Could not find pod metadata")

        svc_port = SECURITY_PROTOCOL_PORTS[self.security_protocol].external

        service = Service(
            metadata=ObjectMeta(
                name=self.service_name,
                namespace=self.broker.unit._backend.model_name,
                ownerReferences=[
                    OwnerReference(
                        apiVersion=self.pod.apiVersion,
                        kind=self.pod.kind,
                        name=self.pod_name,
                        uid=self.pod.metadata.uid,
                        blockOwnerDeletion=False,
                    )
                ],
            ),
            spec=ServiceSpec(
                externalTrafficPolicy="Local",
                type="NodePort",
                selector={"statefulset.kubernetes.io/pod-name": self.pod_name},
                ports=[
                    ServicePort(
                        protocol="TCP",
                        port=svc_port,
                        targetPort=svc_port,
                        name=f"{self.service_name}-port",
                    ),
                ],
            ),
        )

        self.client.apply(service)  # pyright: ignore[reportAttributeAccessIssue]

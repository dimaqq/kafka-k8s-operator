#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Kubernetes resources for a single Kafka pod."""

import logging
from functools import cached_property
from typing import TYPE_CHECKING

from lightkube.core.client import Client
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.core_v1 import Node, Pod, Service

from literals import AuthMechanism

logger = logging.getLogger(__name__)

# default logging from lightkube httpx requests is very noisy
logging.getLogger("lightkube").disabled = True
logging.getLogger("lightkube.core.client").disabled = True
logging.getLogger("httpx").disabled = True

if TYPE_CHECKING:
    pass


class K8sManager:
    """Manager for handling Kafka Kubernetes resources for a single Kafka pod."""

    def __init__(
        self,
        pod_name: str,
        namespace: str,
    ):
        self.pod_name = pod_name
        self.namespace = namespace

    @cached_property
    def client(self) -> Client:
        """The Lightkube client."""
        return Client(  # pyright: ignore[reportArgumentType]
            field_manager=self.pod_name,
            namespace=self.namespace,
        )

    # --- GETTERS ---

    def get_pod(self, pod_name: str = "") -> Pod:
        """Gets the Pod via the K8s API."""
        # Allows us to get pods from other peer units
        pod_name = pod_name or self.pod_name

        return self.client.get(
            res=Pod,
            name=self.pod_name,
        )

    def get_node(self, pod: Pod) -> Node:
        """Gets the Node the Pod is running on via the K8s API."""
        if not pod.spec or not pod.spec.nodeName:
            raise Exception("Could not find podSpec or nodeName")

        return self.client.get(
            Node,
            name=pod.spec.nodeName,
        )

    def get_node_ip(self, node: Node) -> str:
        """Gets the IP Address of the Node via the K8s API."""
        # all these redundant checks are because Lightkube's typing is awful
        if not node.status or not node.status.addresses:
            raise Exception(f"No status found for {node}")

        for addresses in node.status.addresses:
            if addresses.type in ["ExternalIP", "InternalIP", "Hostname"]:
                return addresses.address

        return ""

    def get_service(self, service_name: str) -> Service | None:
        """Gets the Service via the K8s API."""
        return self.client.get(
            res=Service,
            name=service_name,
        )

    def get_node_port(self, service: Service) -> int:
        """Gets the NodePort number for the service via the K8s API."""
        if not service.spec or not service.spec.ports:
            raise Exception("Could not find Service spec or ports")

        return service.spec.ports[0].nodePort

    def build_service_name(self, auth_mechanism: AuthMechanism):
        """Builds the Service name for a given auth.mechanism."""
        return f"{self.pod_name}-{auth_mechanism.lower().replace('_','-')}"

    def build_bootstrap_service_name(self):
        """Builds the Service name for bootstrap-server."""
        return f"{self.pod_name}-bootstrap"

    def get_listener_nodeport(self, auth_mechanism: AuthMechanism) -> int:
        """Gets the current NodePort for the desired auth.mechanism service."""
        service_name = self.build_service_name(auth_mechanism)
        if not (service := self.get_service(service_name)):
            raise Exception(f"Unable to find Service using {auth_mechanism}")

        return self.get_node_port(service)

    def apply_service(self, svc_port: int, service_name: str, nodeport: int | None = None) -> None:
        """Creates a NodePort service for initial client connection.

        In order to discover all Kafka brokers, a client application must know the location of at least 1
        active broker, `bootstrap-server`. From there, the broker returns the `advertised.listeners`
        to the client application, here specified as <NODE-IP>:<NODE-PORT>.

        K8s-external requests hit <NODE-IP>:<NODE-PORT>, and are redirected to the corresponding
        statefulset.kubernetes.io/pod-name from the selector, and port matching the auth mechanism.

        If a pod was rescheduled to a new node, the node-ip defined in the `advertised.listeners`
        will be updated during the normal charm `config-changed` reconciliation.
        """
        pod = self.get_pod(pod_name=self.pod_name)
        if not pod.metadata:
            raise Exception(f"Could not find metadata for {pod}")

        service = Service(
            metadata=ObjectMeta(
                name=service_name,
                namespace=self.namespace,
                ownerReferences=[
                    OwnerReference(
                        apiVersion=pod.apiVersion,
                        kind=pod.kind,
                        name=self.pod_name,
                        uid=pod.metadata.uid,
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
                        name=f"{service_name}-port",
                        nodePort=nodeport,
                    ),
                ],
            ),
        )

        self.client.apply(service)  # pyright: ignore[reportAttributeAccessIssue]

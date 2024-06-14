#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Kubernetes resources."""

import logging
from functools import cached_property

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.core_v1 import Node, Pod, Service

from core.cluster import ClusterState
from literals import SECURITY_PROTOCOL_PORTS, AuthMechanism

logger = logging.getLogger(__name__)

# default logging from lightkube httpx requests is very noisy
logging.getLogger("lightkube").disabled = True
logging.getLogger("lightkube.core.client").disabled = True
logging.getLogger("httpx").disabled = True


class K8sManager:
    """Manager for handling Kafka Kubernetes resources."""

    def __init__(self, state: ClusterState, auth_mechanism: AuthMechanism = "SASL_PLAINTEXT"):
        self.state = state
        self._auth_mechanism: AuthMechanism = auth_mechanism

    @cached_property
    def client(self) -> Client:
        """The Lightkube client."""
        return Client(  # pyright: ignore[reportArgumentType]
            field_manager=self.state.cluster.app.name,
            namespace=self.state.model.name,
        )

    @property
    def auth_mechanism(self) -> AuthMechanism:
        """The auth mechanism for the service."""
        return self._auth_mechanism

    @auth_mechanism.setter
    def auth_mechanism(self, value: AuthMechanism) -> None:
        """Internal auth_mechanism setter."""
        self._auth_mechanism = value

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
    def pod_name(self) -> str:
        """The name of the K8s pod."""
        return self.state.unit_broker.unit.name.replace("/", "-")

    @property
    def service_name(self) -> str:
        """Builds the service name for a given auth mechanism."""
        return f"{self.pod_name}-{self.auth_mechanism.lower()}"

    @property
    def node_port(self) -> int:
        """Builds the complete nodePort for the current unit."""
        return self.state.unit_broker.base_node_port + self.get_auth_mechanism_port_offset(
            self.auth_mechanism
        )

    @staticmethod
    def get_auth_mechanism_port_offset(auth_mechanism: AuthMechanism) -> int:
        """The port offset for different auth mechanisms."""
        # NOTE - this limits us to 99 brokers and 9 auth mechanisms, which is probably fine for now
        # we can revisit this later if needed
        return list(SECURITY_PROTOCOL_PORTS.keys()).index(auth_mechanism)

    def create_nodeport_service(self) -> None:
        """Creates a NodePort service for external access to the current running unit.

        In order to discover all Kafka brokers, a client application must know the location of at least 1
        active broker, `bootstrap-server`. From there, the broker returns the `advertised.listeners`
        to the client application, here specified as <NODE-IP>:<NODE-PORT>.

        K8s-external requests hit <NODE-IP>:<NODE-PORT>, and are redirected to the corresponding
        statefulset.kubernetes.io/pod-name from the selector, and port matching the auth mechanism.

        If a pod was rescheduled to a new node, the node-ip defined in the `advertised.listeners`
        will be updated during the normal charm `config-changed` reconciliation.

        NodePod will be assigned based on the auth mechanism and broker number.
        For example, kafka-k8s/1 running with SASL_SSL, will have nodePort 31011:
            + 30000 (minimum nodePort k8s permits)
            + 1000 (Kafka's application port offset)
            + 10 (kafka unit 1)
            + 1 (SASL_PLAINTEXT auth mechanism)
        """
        if not self.pod.metadata:
            raise Exception("Could not find pod metadata")

        svc_port = SECURITY_PROTOCOL_PORTS[self.auth_mechanism].external

        service = Service(
            metadata=ObjectMeta(
                name=self.service_name,
                namespace=self.state.model.name,
                ownerRefereces=OwnerReference(
                    apiVersion=self.pod.metadata.apiVerison,
                    kind=self.pod.kind,
                    name=self.pod_name,
                    uid=self.pod.metadata.uid,
                    blockOwnerDeletion=False,
                ),
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
                        nodePort=self.node_port,
                        name=f"{self.state.cluster.app.name}-{self.auth_mechanism.lower()}-port",
                    ),
                ],
            ),
        )

        self.client.apply(service)  # pyright: ignore[reportAttributeAccessIssue]

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

    # TODO: check this actually deletes after pod removed
    # def delete_external_services(self) -> None:
    #     """Deletes the NodePort services for the current running unit for when it is shutting down."""
    #     self.client.delete(res=Service, name=self.service_name, namespace=self.state.model.name, cascade=CascadeType.FOREGROUND)

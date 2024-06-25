#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Objects representing the state of KafkaCharm."""

import os
from functools import cached_property
from ipaddress import IPv4Address, IPv6Address

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
    KafkaProviderData,
)
from ops import Framework, Object, Relation
from ops.model import Unit

from core.models import KafkaBroker, KafkaClient, KafkaCluster, ZooKeeper
from core.structured_config import CharmConfig
from literals import (
    INTERNAL_USERS,
    PEER,
    REL_NAME,
    SECRETS_UNIT,
    SECURITY_PROTOCOL_PORTS,
    ZK,
    AuthMechanism,
    Status,
    Substrates,
)

# uncomfortable circular import necessary, as node information cannot live in Juju state


class ClusterState(Object):
    """Collection of global cluster state for the Kafka services."""

    def __init__(self, charm: Framework | Object, substrate: Substrates, config: CharmConfig):
        super().__init__(parent=charm, key="charm_state")
        self.substrate: Substrates = substrate
        self.config = config

        self.peer_app_interface = DataPeerData(self.model, relation_name=PEER)
        self.peer_unit_interface = DataPeerUnitData(
            self.model, relation_name=PEER, additional_secret_fields=SECRETS_UNIT
        )
        self.zookeeper_requires_interface = DatabaseRequirerData(
            self.model, relation_name=ZK, database_name=f"/{self.model.app.name}"
        )
        self.client_provider_interface = KafkaProviderData(self.model, relation_name=REL_NAME)

    # --- RELATIONS ---

    @property
    def peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER)

    @property
    def zookeeper_relation(self) -> Relation | None:
        """The ZooKeeper relation."""
        return self.model.get_relation(ZK)

    @property
    def client_relations(self) -> set[Relation]:
        """The relations of all client applications."""
        return set(self.model.relations[REL_NAME])

    # --- CORE COMPONENTS ---

    @property
    def unit_broker(self) -> KafkaBroker:
        """The broker state of the current running Unit."""
        return KafkaBroker(
            relation=self.peer_relation,
            data_interface=self.peer_unit_interface,
            component=self.model.unit,
            substrate=self.substrate,
        )

    @cached_property
    def peer_units_data_interfaces(self) -> dict[Unit, DataPeerOtherUnitData]:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        return {
            unit: DataPeerOtherUnitData(model=self.model, unit=unit, relation_name=PEER)
            for unit in self.peer_relation.units
        }

    @property
    def cluster(self) -> KafkaCluster:
        """The cluster state of the current running App."""
        return KafkaCluster(
            relation=self.peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
            substrate=self.substrate,
        )

    @property
    def brokers(self) -> set[KafkaBroker]:
        """Grabs all servers in the current peer relation, including the running unit server.

        Returns:
            Set of KafkaBrokers in the current peer relation, including the running unit server.
        """
        brokers = set()
        for unit, data_interface in self.peer_units_data_interfaces.items():
            brokers.add(
                KafkaBroker(
                    relation=self.peer_relation,
                    data_interface=data_interface,
                    component=unit,
                    substrate=self.substrate,
                )
            )
        brokers.add(self.unit_broker)

        return brokers

    @property
    def zookeeper(self) -> ZooKeeper:
        """The ZooKeeper relation state."""
        return ZooKeeper(
            relation=self.zookeeper_relation,
            data_interface=self.zookeeper_requires_interface,
            substrate=self.substrate,
            local_app=self.cluster.app,
        )

    @property
    def clients(self) -> set[KafkaClient]:
        """The state for all related client Applications."""
        clients = set()
        for relation in self.client_relations:
            if not relation.app:
                continue

            clients.add(
                KafkaClient(
                    relation=relation,
                    data_interface=self.client_provider_interface,
                    component=relation.app,
                    substrate=self.substrate,
                    local_app=self.cluster.app,
                    bootstrap_server=self.bootstrap_server,
                    password=self.cluster.client_passwords.get(f"relation-{relation.id}", ""),
                    tls="enabled" if self.cluster.tls_enabled else "disabled",
                    zookeeper_uris=self.zookeeper.uris,
                )
            )

        return clients

    # ---- GENERAL VALUES ----

    @property
    def bind_address(self) -> IPv4Address | IPv6Address | str:
        """The network binding address from the peer relation."""
        bind_address = None
        if self.peer_relation:
            if binding := self.model.get_binding(self.peer_relation):
                bind_address = binding.network.bind_address

        return bind_address or ""

    @property
    def super_users(self) -> str:
        """Generates all users with super/admin permissions for the cluster from relations.

        Formatting allows passing to the `super.users` property.

        Returns:
            Semicolon delimited string of current super users
        """
        super_users = set(INTERNAL_USERS)
        for relation in self.client_relations:
            if not relation or not relation.app:
                continue

            extra_user_roles = relation.data[relation.app].get("extra-user-roles", "")
            password = self.cluster.relation_data.get(f"relation-{relation.id}", None)
            # if passwords are set for client admins, they're good to load
            if "admin" in extra_user_roles and password is not None:
                super_users.add(f"relation-{relation.id}")

        super_users_arg = sorted([f"User:{user}" for user in super_users])

        return ";".join(super_users_arg)

    @property
    def security_protocol(self) -> AuthMechanism:
        """The current enabled :class:`AuthMechanism` for bootstrap."""
        return (
            "SASL_SSL"
            if self.cluster.tls_enabled and self.unit_broker.certificate
            else "SASL_PLAINTEXT"
        )

    @property
    def bootstrap_servers_external(self) -> str:
        """Comma-delimited string of `bootstrap-server` for external access."""
        return ",".join(sorted([broker.bootstrap_server_external for broker in self.brokers]))

    @property
    def bootstrap_server(self) -> str:
        """The current Kafka uris formatted for the `bootstrap-server` command flag.

        Returns:
            List of `bootstrap-server` servers
        """
        if not self.peer_relation:
            return ""

        if self.config.expose_nodeport:  # implicitly checks for k8s in structured_config
            return self.bootstrap_servers_external

        return ",".join(
            sorted(
                [
                    f"{broker.internal_address}:{SECURITY_PROTOCOL_PORTS[self.security_protocol]}"
                    for broker in self.brokers
                ]
            )
        )

    @property
    def log_dirs(self) -> str:
        """Builds the necessary log.dirs based on mounted storage volumes.

        Returns:
            String of log.dirs property value to be set
        """
        return ",".join([os.fspath(storage.location) for storage in self.model.storages["data"]])

    @property
    def planned_units(self) -> int:
        """Return the planned units for the charm."""
        return self.model.app.planned_units()

    @property
    def ready_to_start(self) -> Status:
        """Check for active ZooKeeper relation and adding of inter-broker auth username.

        Returns:
            True if ZK is related and `sync` user has been added. False otherwise.
        """
        if not self.peer_relation:
            return Status.NO_PEER_RELATION

        if not self.zookeeper or not self.zookeeper.relation:
            return Status.ZK_NOT_RELATED

        if not self.zookeeper.zookeeper_connected:
            return Status.ZK_NO_DATA

        # TLS must be enabled for Kafka and ZK or disabled for both
        if self.cluster.tls_enabled ^ self.zookeeper.tls:
            return Status.ZK_TLS_MISMATCH

        if self.cluster.tls_enabled and not self.unit_broker.certificate:
            return Status.NO_CERT

        if not self.cluster.internal_user_credentials:
            return Status.NO_BROKER_CREDS

        return Status.ACTIVE

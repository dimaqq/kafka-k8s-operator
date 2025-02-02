#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache Kafka."""

import logging

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops import (
    ActiveStatus,
    EventBase,
    InstallEvent,
    SecretChangedEvent,
    StartEvent,
    StatusBase,
    StorageAttachedEvent,
    StorageDetachingEvent,
    UpdateStatusEvent,
    pebble,
)
from ops.main import main
from ops.pebble import Layer

from core.cluster import ClusterState
from core.structured_config import CharmConfig
from events.oauth import OAuthHandler
from events.password_actions import PasswordActionEvents
from events.provider import KafkaProvider
from events.tls import TLSHandler
from events.upgrade import KafkaDependencyModel, KafkaUpgrade
from events.zookeeper import ZooKeeperHandler
from literals import (
    CHARM_KEY,
    CONTAINER,
    DEPENDENCIES,
    GROUP,
    JMX_EXPORTER_PORT,
    LOGS_RULES_DIR,
    METRICS_RULES_DIR,
    PEER,
    REL_NAME,
    SUBSTRATE,
    USER,
    DebugLevel,
    Status,
    Substrates,
)
from managers.auth import AuthManager
from managers.config import KafkaConfigManager
from managers.tls import TLSManager
from workload import KafkaWorkload

logger = logging.getLogger(__name__)


class KafkaCharm(TypedCharmBase[CharmConfig]):
    """Charmed Operator for Kafka K8s."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.substrate: Substrates = SUBSTRATE
        self.workload = KafkaWorkload(container=self.unit.get_container(CONTAINER))
        self.state = ClusterState(self, substrate=self.substrate)

        # HANDLERS

        self.password_action_events = PasswordActionEvents(self)
        self.zookeeper = ZooKeeperHandler(self)
        self.tls = TLSHandler(self)
        self.oauth = OAuthHandler(self)
        self.provider = KafkaProvider(self)
        self.upgrade = KafkaUpgrade(
            self,
            substrate=self.substrate,
            dependency_model=KafkaDependencyModel(
                **DEPENDENCIES  # pyright: ignore[reportArgumentType]
            ),
        )

        # MANAGERS

        self.config_manager = KafkaConfigManager(
            state=self.state,
            workload=self.workload,
            config=self.config,
            current_version=self.upgrade.current_version,
        )
        self.tls_manager = TLSManager(
            state=self.state, workload=self.workload, substrate=self.substrate
        )
        self.auth_manager = AuthManager(
            state=self.state, workload=self.workload, kafka_opts=self.config_manager.kafka_opts
        )

        self.restart = RollingOpsManager(self, relation="restart", callback=self._restart)
        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            jobs=[{"static_configs": [{"targets": [f"*:{JMX_EXPORTER_PORT}"]}]}],
            alert_rules_path=METRICS_RULES_DIR,
        )
        self.grafana_dashboards = GrafanaDashboardProvider(self)
        self.loki_push = LogProxyConsumer(
            self,
            log_files=[f"{self.workload.paths.logs_path}/server.log"],
            alert_rules_path=LOGS_RULES_DIR,
            relation_name="logging",
            container_name="kafka",
        )

        self.framework.observe(getattr(self.on, "install"), self._on_install)
        self.framework.observe(getattr(self.on, "start"), self._on_start)
        self.framework.observe(getattr(self.on, "kafka_pebble_ready"), self._on_kafka_pebble_ready)
        self.framework.observe(getattr(self.on, "config_changed"), self._on_config_changed)
        self.framework.observe(getattr(self.on, "update_status"), self._on_update_status)
        self.framework.observe(getattr(self.on, "secret_changed"), self._on_secret_changed)

        self.framework.observe(self.on[PEER].relation_changed, self._on_config_changed)

        self.framework.observe(
            getattr(self.on, "data_storage_attached"), self._on_storage_attached
        )
        self.framework.observe(
            getattr(self.on, "data_storage_detaching"), self._on_storage_detaching
        )

    @property
    def _kafka_layer(self) -> Layer:
        """Returns a Pebble configuration layer for Kafka."""
        extra_opts = [
            f"-javaagent:{self.workload.paths.jmx_prometheus_javaagent}={JMX_EXPORTER_PORT}:{self.workload.paths.jmx_prometheus_config}",
            f"-Djava.security.auth.login.config={self.workload.paths.zk_jaas}",
        ]
        command = f"{self.workload.paths.binaries_path}/bin/kafka-server-start.sh {self.workload.paths.server_properties}"

        layer_config: pebble.LayerDict = {
            "summary": "kafka layer",
            "description": "Pebble config layer for kafka",
            "services": {
                CONTAINER: {
                    "override": "replace",
                    "summary": "kafka",
                    "command": command,
                    "startup": "enabled",
                    "user": USER,
                    "group": GROUP,
                    "environment": {
                        "KAFKA_OPTS": " ".join(extra_opts),
                        # FIXME https://github.com/canonical/kafka-k8s-operator/issues/80
                        "JAVA_HOME": "/usr/lib/jvm/java-18-openjdk-amd64",
                        "LOG_DIR": self.workload.paths.logs_path,
                    },
                }
            },
        }
        return Layer(layer_config)

    def _on_kafka_pebble_ready(self, event: EventBase) -> None:
        """Handler for `start` event."""
        # don't want to run default pebble ready during upgrades
        if not self.upgrade.idle:
            return

        self._set_status(self.state.ready_to_start)
        if not isinstance(self.unit.status, ActiveStatus):
            event.defer()
            return

        # required settings given zookeeper connection config has been created
        self.config_manager.set_server_properties()
        self.config_manager.set_zk_jaas_config()
        self.config_manager.set_client_properties()

        # during pod-reschedules (e.g upgrades or otherwise) we lose all files
        # need to manually add-back key/truststores
        if (
            self.state.cluster.tls_enabled
            and self.state.unit_broker.certificate
            and self.state.unit_broker.ca
        ):  # TLS is probably completed
            self.tls_manager.set_server_key()
            self.tls_manager.set_ca()
            self.tls_manager.set_certificate()
            self.tls_manager.set_truststore()
            self.tls_manager.set_keystore()

        # start kafka service
        self.workload.start(layer=self._kafka_layer)
        logger.info("Kafka service started")

        # service_start might fail silently, confirm with ZK if kafka is actually connected
        self.on.update_status.emit()

    def _on_start(self, event: StartEvent) -> None:
        """Wrapper for start event."""
        self._on_kafka_pebble_ready(event)

    def _on_install(self, event: InstallEvent) -> None:
        """Generate internal passwords for the application."""
        if not self.unit.get_container(CONTAINER).can_connect():
            event.defer()
            return

        self.unit.set_workload_version(self.workload.get_version())
        self.config_manager.set_environment()

    def _on_config_changed(self, event: EventBase) -> None:
        """Generic handler for most `config_changed` events across relations."""
        if not self.upgrade.idle or not self.healthy:
            event.defer()
            return

        # Load current properties set in the charm workload
        properties = self.workload.read(self.workload.paths.server_properties)
        properties_changed = set(properties) ^ set(self.config_manager.server_properties)

        zk_jaas = self.workload.read(self.workload.paths.zk_jaas)
        zk_jaas_changed = set(zk_jaas) ^ set(self.config_manager.zk_jaas_config.splitlines())

        if not properties or not zk_jaas:
            # Event fired before charm has properly started
            event.defer()
            return

        # update environment
        self.config_manager.set_environment()
        self.unit.set_workload_version(self.workload.get_version())

        if zk_jaas_changed:
            clean_broker_jaas = [conf.strip() for conf in zk_jaas]
            clean_config_jaas = [
                conf.strip() for conf in self.config_manager.zk_jaas_config.splitlines()
            ]
            logger.info(
                (
                    f'Broker {self.unit.name.split("/")[1]} updating JAAS config - '
                    f"OLD JAAS = {set(clean_broker_jaas) - set(clean_config_jaas)}, "
                    f"NEW JAAS = {set(clean_config_jaas) - set(clean_broker_jaas)}"
                )
            )
            self.config_manager.set_zk_jaas_config()

        if properties_changed:
            logger.info(
                (
                    f'Broker {self.unit.name.split("/")[1]} updating config - '
                    f"OLD PROPERTIES = {set(properties) - set(self.config_manager.server_properties)}, "
                    f"NEW PROPERTIES = {set(self.config_manager.server_properties) - set(properties)}"
                )
            )
            self.config_manager.set_server_properties()

        if zk_jaas_changed or properties_changed:
            self.on[f"{self.restart.name}"].acquire_lock.emit()

        # update client_properties whenever possible
        self.config_manager.set_client_properties()

        # If Kafka is related to client charms, update their information.
        if self.model.relations.get(REL_NAME, None) and self.unit.is_leader():
            self.update_client_data()

    def _on_update_status(self, _: UpdateStatusEvent) -> None:
        """Handler for `update-status` events."""
        if not self.upgrade.idle or not self.healthy:
            return

        if not self.state.zookeeper.broker_active():
            self._set_status(Status.ZK_NOT_CONNECTED)
            return

        # NOTE for situations like IP change and late integration with rack-awareness charm.
        # If properties have changed, the broker will restart.
        self.on.config_changed.emit()

        self._set_status(Status.ACTIVE)

    def _on_secret_changed(self, event: SecretChangedEvent) -> None:
        """Handler for `secret_changed` events."""
        if not event.secret.label or not self.state.cluster.relation:
            return

        if event.secret.label == self.state.cluster.data_interface._generate_secret_label(
            PEER,
            self.state.cluster.relation.id,
            "extra",  # pyright: ignore[reportArgumentType] -- Changes with the https://github.com/canonical/data-platform-libs/issues/124
        ):
            # TODO: figure out why creating internal credentials setting doesn't trigger changed event here
            self.on.config_changed.emit()

    def _on_storage_attached(self, event: StorageAttachedEvent) -> None:
        """Handler for `storage_attached` events."""
        # checks first whether the broker is active before warning
        if self.workload.active():
            # new dirs won't be used until topic partitions are assigned to it
            # either automatically for new topics, or manually for existing
            self._set_status(Status.ADDED_STORAGE)
            self._on_config_changed(event)

    def _on_storage_detaching(self, _: StorageDetachingEvent) -> None:
        """Handler for `storage_detaching` events."""
        # in the case where there may be replication recovery may be possible
        if self.state.peer_relation and len(self.state.peer_relation.units):
            self._set_status(Status.REMOVED_STORAGE)
        else:
            self._set_status(Status.REMOVED_STORAGE_NO_REPL)

        self.on.config_changed.emit()

    def _restart(self, event: EventBase) -> None:
        """Handler for `rolling_ops` restart events."""
        # only attempt restart if service is already active
        if not self.healthy:
            event.defer()
            return

        self.workload.restart()

        if self.healthy:
            logger.info(f'Broker {self.unit.name.split("/")[1]} restarted')
        else:
            logger.error(f"Broker {self.unit.name.split('/')[1]} failed to restart")

    @property
    def healthy(self) -> bool:
        """Checks and updates various charm lifecycle states.

        Is slow to fail due to retries, to be used sparingly.

        Returns:
            True if service is alive and active. Otherwise False
        """
        self._set_status(self.state.ready_to_start)
        if not isinstance(self.unit.status, ActiveStatus):
            return False

        if not self.workload.active():
            self._set_status(Status.SERVICE_NOT_RUNNING)
            return False

        return True

    def update_client_data(self) -> None:
        """Writes necessary relation data to all related client applications."""
        if not self.unit.is_leader() or not self.healthy:
            return

        for client in self.state.clients:
            if not client.password:
                logger.debug(
                    f"Skipping update of {client.app.name}, user has not yet been added..."
                )
                continue

            client.update(
                {
                    "endpoints": client.bootstrap_server,
                    "zookeeper-uris": client.zookeeper_uris,
                    "consumer-group-prefix": client.consumer_group_prefix,
                    "topic": client.topic,
                    "username": client.username,
                    "password": client.password,
                    "tls": client.tls,
                    "tls-ca": client.tls,  # TODO: fix tls-ca
                }
            )

    def _set_status(self, key: Status) -> None:
        """Sets charm status."""
        status: StatusBase = key.value.status
        log_level: DebugLevel = key.value.log_level

        getattr(logger, log_level.lower())(status.message)
        self.unit.status = status


if __name__ == "__main__":
    main(KafkaCharm)

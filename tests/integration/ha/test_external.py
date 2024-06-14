#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import pytest
from pytest_operator.plugin import OpsTest

from integration.helpers import (
    APP_NAME,
    DUMMY_NAME,
    KAFKA_CONTAINER,
    REL_NAME_ADMIN,
    ZK_NAME,
    get_nodeport_services,
)

logger = logging.getLogger(__name__)

TEST_NAME = "kafka-test-app"
TEST_TOPIC = "HOT-TOPIC"
test_charm_config = {"role": "producer", "topic_name": TEST_TOPIC, "num_messages": 100000}


@pytest.mark.skip_if_deployed
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, kafka_charm, app_charm):
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_charm,
            application_name=APP_NAME,
            num_units=3,
            resources={"kafka-image": KAFKA_CONTAINER},
            config={"expose-nodeport": False},
        ),
        ops_test.model.deploy(ZK_NAME, channel="3/edge", num_units=1),
        ops_test.model.deploy(app_charm, application_name=DUMMY_NAME),
    )
    await ops_test.model.wait_for_idle(apps=[APP_NAME, ZK_NAME], timeout=2000)

    await ops_test.model.add_relation(APP_NAME, ZK_NAME)
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(90)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, ZK_NAME],
        idle_period=30,
        status="active",
        timeout=2000,
        raise_on_error=False,
    )

    await ops_test.model.add_relation(APP_NAME, f"{DUMMY_NAME}:{REL_NAME_ADMIN}")

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, DUMMY_NAME, ZK_NAME], idle_period=30, status="active", timeout=2000
        )

    assert not get_nodeport_services(ops_test)


@pytest.mark.abort_on_fail
async def test_expose_nodeport(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].set_config({"expose-nodeport": True})

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], idle_period=30, status="active", timeout=2000
    )

    np_services = get_nodeport_services(ops_test)

    assert np_services  # checks any service exist at all
    assert len(np_services) == len(
        ops_test.model.applications[APP_NAME].units
    )  # check all brokers have a service
    assert (
        len({np_service[4] for np_service in np_services}) == 3
    )  # checks all services have unique nodePorts

    for selector in [np_service[6] for np_service in np_services]:
        assert (
            "statefulset.kubernetes.io/pod-name" in selector
        )  # checks all services have necessary selector


# @pytest.mark.abort_on_fail
# async def test_external_access_sasl_plaintext(ops_test: OpsTest):
#     bootstrap_node_port = int(get_nodeport_services(ops_test)[0].split()[5].split(":")[1].split("/")[0])
#     node_ip = get_node_ip(ops_test)

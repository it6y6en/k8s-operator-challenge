#!/usr/bin/env python3
"""
PingPong operator (kopf)

Creates:
 - ConfigMap with config.yaml
 - headless Service for pod to pod communication
 - StatefulSet and mounts ConfigMap to -> /var/server/config.yaml

On update:
 - updates statefulset (replica, image change)
 - updates ConfigMap
"""

import kopf
import kubernetes
import logging
from kubernetes.client.rest import ApiException

GROUP = "apps.example.com"
VERSION = "v1alpha1"
PLURAL = "pingpongs"

CONTAINER_PORT = 80
CONFIG_FILENAME = "config.yaml"
MOUNT_PATH = "/var/server"
CONFIG_KEY = "config.yaml"

# Initialize kubernetes client
kubernetes.config.load_incluster_config()
core = kubernetes.client.CoreV1Api()
apps = kubernetes.client.AppsV1Api()

def configmap_manifest(name, namespace, replicas, timer, labels):
    data = f"replicas: {replicas}\ntimer: {timer}\n"
    return kubernetes.client.V1ConfigMap(
        metadata=kubernetes.client.V1ObjectMeta(name=name, namespace=namespace, labels=labels),
        data={CONFIG_KEY: data},
    )


def service_manifest(name, namespace, labels):
    spec = kubernetes.client.V1ServiceSpec(
        cluster_ip="None",
        selector=labels,
        ports=[kubernetes.client.V1ServicePort(port=CONTAINER_PORT, target_port=CONTAINER_PORT, protocol="TCP")],
    )
    return kubernetes.client.V1Service(
        metadata=kubernetes.client.V1ObjectMeta(name=name, namespace=namespace, labels=labels),
        spec=spec,
    )


def statefulset_manifest(name, service_name, configmap_name, namespace, labels, replicas, image):
    container = kubernetes.client.V1Container(
        name="main",
        image=image,
        env=[
            kubernetes.client.V1EnvVar(
                name="HEADLESS_SERVICE",
                value=service_name
            )
        ],
        ports=[kubernetes.client.V1ContainerPort(container_port=CONTAINER_PORT)],
        volume_mounts=[
            kubernetes.client.V1VolumeMount(
                name="config-volume",
                mount_path=MOUNT_PATH,
                read_only=True,
            )
        ],
        resources=kubernetes.client.V1ResourceRequirements(
            requests={
                "cpu": "100m",
                "memory": "64Mi"
            },
            limits={
                "cpu": "100m",
                "memory": "64Mi"
            },
        ),
    )

    volumes = [
        kubernetes.client.V1Volume(
            name="config-volume",
            config_map=kubernetes.client.V1ConfigMapVolumeSource(name=configmap_name, items=[
                kubernetes.client.V1KeyToPath(key=CONFIG_KEY, path=CONFIG_KEY)
            ]),
        )
    ]

    pod_template = kubernetes.client.V1PodTemplateSpec(
        metadata=kubernetes.client.V1ObjectMeta(labels=labels),
        spec=kubernetes.client.V1PodSpec(containers=[container], volumes=volumes)
    )

    spec = kubernetes.client.V1StatefulSetSpec(
        service_name=service_name,
        replicas=replicas,
        selector=kubernetes.client.V1LabelSelector(match_labels=labels),
        template=pod_template,
    )

    return kubernetes.client.V1StatefulSet(
        metadata=kubernetes.client.V1ObjectMeta(name=name, namespace=namespace, labels=labels),
        spec=spec,
    )


@kopf.on.create(GROUP, VERSION, PLURAL)
def create_fn(spec, name, namespace, logger, **kwargs):
    replicas = int(spec.get("replicas", 1))
    timer = int(spec.get("timer", 30))
    image = spec.get('image', '6y6en/ping-pong:latest') 

    # resources will be named based on CR name
    svc_name = f"{name}-svc"
    sts_name = f"{name}-sts"
    cm_name = f"{name}-cm"
    labels = {"app": name}

    logger.info(f"Creating PingPong resources: svc={svc_name}, sts={sts_name}, cm={cm_name}, replicas={replicas}, timer={timer}")

    # 1) create headless service
    svc_manifest = service_manifest(svc_name, namespace, labels)
    try:
        core.create_namespaced_service(namespace=namespace, body=svc_manifest)
        logger.info(f"Service {svc_name} created")
    except ApiException as e:
        if e.status == 409:
            logger.info(f"Service {svc_name} already exists")
        else:
            raise

    # 2) create configmap
    cm_manifest = configmap_manifest(cm_name, namespace, replicas, timer, labels)
    try:
        core.create_namespaced_config_map(namespace=namespace, body=cm_manifest)
        logger.info(f"ConfigMap {cm_name} created")
    except ApiException as e:
        if e.status == 409:
            logger.info(f"ConfigMap {cm_name} already exists")
        else:
            raise

    # 3) create statefulset
    sts_manifest = statefulset_manifest(sts_name, svc_name, cm_name, namespace, labels, replicas, image)
    try:
        apps.create_namespaced_stateful_set(namespace=namespace, body=sts_manifest)
        logger.info(f"StatefulSet {sts_name} created")
    except ApiException as e:
        if e.status == 409:
            logger.info(f"StatefulSet {sts_name} already exists")
        else:
            raise

    # store names for later handlers
    return {"svc_name": svc_name, "sts_name": sts_name, "cm_name": cm_name}


@kopf.on.update(GROUP, VERSION, PLURAL)
def update_fn(spec, name, namespace, diff, logger, **kwargs):
    image = spec.get('image')
    replicas = spec.get('replicas')
    timer = spec.get('timer')

    sts_name = f"{name}-sts"
    cm_name = f"{name}-cm"

    for change in diff:
        field, old, new = change[1], change[2], change[3]
        logger.info(f"Detected change in {field}: {old} -> {new}")

    ss_body = {"spec": {
                "replicas": replicas,
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "main", "image": image}
                        ]
                    }
                }
            },
        }

    try:
        apps.patch_namespaced_stateful_set(sts_name, namespace, ss_body)
        logging.info(f"Updated StatefulSet {sts_name}")
    except ApiException as e:
        logging.error(f"Failed to update StatefulSet: {e}")

    cm_data = f"replicas: {replicas}\ntimer: {timer}\n"
    cm_body = {"data": {CONFIG_KEY: cm_data}}
    
    try:
        core.patch_namespaced_config_map(cm_name, namespace, cm_body)
        logging.info(f"Updated ConfigMap {cm_name}")
    except ApiException as e:
        logging.error(f"Failed to update ConfigMap: {e}")


@kopf.on.delete(GROUP, VERSION, PLURAL)
def delete_fn(spec, name, namespace, logger, **kwargs):
    # Optionally delete associated k8s resources (svc, sts, cm).
    # Here we attempt to delete them; ignore NotFound errors.
    svc_name = f"{name}-svc"
    sts_name = f"{name}-sts"
    cm_name = f"{name}-cm"

    for fn, n in [
        (core.delete_namespaced_service, svc_name),
        (apps.delete_namespaced_stateful_set, sts_name),
        (core.delete_namespaced_config_map, cm_name),
    ]:
        try:
            fn(name=n, namespace=namespace)
            logger.info(f"Deleted {n}")
        except ApiException as e:
            if e.status == 404:
                logger.info(f"{n} already deleted")
            else:
                logger.error(f"Failed to delete {n}: {e}")

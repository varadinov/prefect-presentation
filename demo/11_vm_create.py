from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

import random
from datetime import datetime, timezone
from typing import TypedDict

from prefect import flow, task


class VmCreateInputs(TypedDict):
    vm_name: str
    tenant_id: str
    tshirt_size: str
    public_ip: bool
    firewall_settings: str


@task(task_run_name="Check if VM exists — {vm_name}", persist_result=True)
def vm_already_exists(vm_name: str) -> bool:
    # Print-only demo: pretend some names are "taken"
    taken = {"demo-vm", "existing-vm", "prod-web-1"}
    exists = vm_name.strip().lower() in taken
    print(f"[check] VM '{vm_name}' already exists? -> {exists}")
    return exists


@task(task_run_name="Stop — conflict", persist_result=True)
def stop_conflict(vm_name: str) -> None:
    print(f"[stop] Conflict: VM '{vm_name}' already exists. Update DB with state conflict and stop.")


@task(task_run_name="Reserve storage — {tshirt_size}", persist_result=True)
def reserve_storage(tshirt_size: str) -> str:
    storage_id = f"stor-{tshirt_size.lower()}-{int(datetime.now(timezone.utc).timestamp())}"
    print(f"[reserve] Storage reserved for size '{tshirt_size}' -> {storage_id}")
    return storage_id


@task(task_run_name="Reserve cluster — tenant {tenant_id}", persist_result=True)
def reserve_cluster(tenant_id: str) -> str:
    cluster_id = f"cluster-{tenant_id[-6:]}"
    print(f"[reserve] Cluster reserved for tenant '{tenant_id}' -> {cluster_id}")
    return cluster_id


@task(task_run_name="Acquire public IP", persist_result=True)
def acquire_ip() -> str:
    ip = "203.0.113.10"  
    print(f"[reserve] Acquired public IP -> {ip}")
    return ip


@task(task_run_name="DB write — in-progress", persist_result=True)
def db_write_in_progress(inputs: VmCreateInputs) -> None:
    print(
        "[db] write in-progress "
        f"(vm_name={inputs['vm_name']}, tenant_id={inputs['tenant_id']}, size={inputs['tshirt_size']}, public_ip={inputs['public_ip']})"
    )


@task(task_run_name="Create volume", persist_result=True)
def create_volume(storage_id: str) -> str:
    volume_id = f"vol-{storage_id}"
    print(f"[create] Volume created from storage reservation '{storage_id}' -> {volume_id}")
    return volume_id


@task(
    task_run_name="Create VM — {vm_name}",
    persist_result=True,
)
def create_vm(vm_name: str, tenant_id: str, tshirt_size: str, cluster_id: str) -> str:
    roll = random.random()
    if roll < 0.8:
        print(f"[create] VM creation failed (simulated), roll={roll:.3f} (fail if < 0.8)")
        raise RuntimeError("Simulated VM creation failure (80% per attempt)")
    vm_id = f"vm-{tenant_id[-6:]}-{vm_name}"
    print(
        f"[create] VM created (name='{vm_name}', tenant='{tenant_id}', size='{tshirt_size}', cluster='{cluster_id}')"
        f" -> {vm_id}"
    )
    return vm_id


@task(task_run_name="Attach volume", persist_result=True)
def attach_volume(vm_id: str, volume_id: str) -> None:
    print(f"[attach] Attached volume '{volume_id}' -> VM '{vm_id}'")


@task(task_run_name="Attach IP", persist_result=True)
def attach_ip(vm_id: str, ip: str) -> None:
    print(f"[attach] Attached IP '{ip}' -> VM '{vm_id}'")


@task(task_run_name="Configure firewall", persist_result=True)
def configure_firewall(vm_id: str, firewall_settings: str) -> None:
    print(f"[config] Firewall configured for VM '{vm_id}' with settings: {firewall_settings}")


@task(task_run_name="DB write — completed", persist_result=True)
def db_write_completed(vm_id: str) -> None:
    print(f"[db] write completed (vm_id={vm_id})")


@flow(flow_run_name=lambda: f"vm-create-{datetime.now(timezone.utc):%Y-%m-%d-%H%M}")
def vm_create_workflow(
    vm_name: str,
    tenant_id: str,
    tshirt_size: str,
    public_ip: bool,
    firewall_settings: str,
) -> None:
    """
    Print-only Prefect demo flow based on the provided diagram:
    """
    inputs: VmCreateInputs = {
        "vm_name": vm_name,
        "tenant_id": tenant_id,
        "tshirt_size": tshirt_size,
        "public_ip": public_ip,
        "firewall_settings": firewall_settings,
    }
    print(
        "[inputs] "
        f"vm_name={inputs['vm_name']}, tenant_id={inputs['tenant_id']}, size={inputs['tshirt_size']}, "
        f"public_ip={inputs['public_ip']}, firewall_settings={inputs['firewall_settings']}"
    )

    if vm_already_exists(inputs["vm_name"]):
        stop_conflict(inputs["vm_name"])
        return

    # "Fan-out" section from the diagram (runs concurrently in Prefect).
    storage_f = reserve_storage.submit(inputs["tshirt_size"])
    cluster_f = reserve_cluster.submit(inputs["tenant_id"])
    ip_f = acquire_ip.submit() if inputs["public_ip"] else None
    db_inprog_f = db_write_in_progress.submit(inputs)

    # Converge point (everything reserved / recorded) before creation sequence.
    storage_id = storage_f.result()
    cluster_id = cluster_f.result()
    if ip_f is not None:
        ip = ip_f.result()
    else:
        ip = None
    db_inprog_f.result()

    volume_id = create_volume(storage_id)
    vm_id = create_vm(
        inputs["vm_name"],
        inputs["tenant_id"],
        inputs["tshirt_size"],
        cluster_id,
    )
    attach_volume(vm_id, volume_id)

    if ip is not None:
        attach_ip(vm_id, ip)
    else:
        print("[attach] Skipping IP attach (public_ip=False)")

    configure_firewall(vm_id, inputs["firewall_settings"])
    db_write_completed(vm_id)


def _default_demo_parameters() -> VmCreateInputs:
    return {
        "vm_name": "vm-demo-1",
        "tenant_id": "tenant-1234567890",
        "tshirt_size": "M",
        "public_ip": True,
        "firewall_settings": "allow: 22/tcp, 80/tcp, 443/tcp; deny: all",
    }


if __name__ == "__main__":
    params = _default_demo_parameters()
    vm_create_workflow.serve(
        name="vm-create-demo",
        parameters=params, # type: ignore
    )

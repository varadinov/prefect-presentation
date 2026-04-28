import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

import time
from prefect import flow, task
from prefect.transactions import transaction

@task(task_run_name="Acquire public IP")
def acquire_public_ip() -> str:
    # Print-only demo: pretend we reserved an IP from an IPAM service.
    ip = "203.0.113.10"
    print(f"[reserve] Acquired public IP -> {ip}")
    return ip


@acquire_public_ip.on_rollback
def release_public_ip(txn):
    ip = txn.get("public_ip")
    if ip:
        print(f"[rollback] Released public IP -> {ip}")


@task(task_run_name="Provision VM — {vm_name}")
def provision_vm(vm_name: str, public_ip: str, should_fail: bool = True) -> str:
    time.sleep(10)
    if should_fail:
        raise RuntimeError("Provisioning failed (simulated)")
    vm_id = f"vm-{vm_name}"
    print(f"[create] Provisioned VM -> {vm_id} (ip={public_ip})")
    return vm_id


@flow(name="transactions-demo", log_prints=True)
def transactions_demo(
    vm_name: str = "vm-demo-1",
    fail_provisioning: bool = True,
) -> str:
    # If provisioning fails, the transaction rolls back and releases the IP.
    with transaction() as txn:
        ip = acquire_public_ip()
        txn.set("public_ip", ip)
        vm_id = provision_vm(vm_name=vm_name, public_ip=ip, should_fail=fail_provisioning)
    return vm_id


def _default_demo_parameters() -> dict:
    return {
        "vm_name": "vm-demo-1",
        "fail_provisioning": True,
    }


if __name__ == "__main__":
    params = _default_demo_parameters()
    transactions_demo.serve(
        name="transactions-demo",
        parameters=params,
    )

#!/usr/bin/env python3
"""Cenário normal: verificação de segurança e ausência de eleições espúrias.

Sobe um cluster estável, observa-o por um intervalo sem injetar falhas e
confirma que (i) existe exatamente um líder, (ii) todos os nós concordam sobre
a identidade do líder e (iii) nenhuma eleição é disparada após a estabilização.
"""

import os
import re
import csv
import json
import time
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
DATA = os.path.join(HERE, "data")
PROJECT = "mc714safety"
OBSERVE = 20.0


def gen_compose(n_nodes=3, hb_timeout=3.0):
    lines = ["services:"]
    for i in range(1, n_nodes + 1):
        peers = ",".join(f"{j}:node{j}:5000" for j in range(1, n_nodes + 1) if j != i)
        lines += [
            f"  node{i}:",
            f"    build: {os.path.join(ROOT, 'src')}",
            f"    container_name: {PROJECT}_node{i}",
            "    environment:",
            f"      - NODE_ID={i}",
            "      - PORT=5000",
            f"      - PEERS={peers}",
            "      - HEARTBEAT_INTERVAL=1.0",
            f"      - HEARTBEAT_TIMEOUT={hb_timeout}",
            "    networks:",
            "      - ds-net",
        ]
    lines += ["networks:", "  ds-net:", "    driver: bridge"]
    with open(os.path.join(HERE, "compose.safety.yml"), "w") as fh:
        fh.write("\n".join(lines) + "\n")


def dc(*a):
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f",
         os.path.join(HERE, "compose.safety.yml"), *a],
        capture_output=True, text=True,
    )


def logs(nid):
    r = subprocess.run(["docker", "logs", f"{PROJECT}_node{nid}"],
                       capture_output=True, text=True)
    return r.stdout + r.stderr


def announces(nid):
    return [(int(m.group(1)), float(m.group(2)))
            for m in re.finditer(r"LEADER_ANNOUNCE node=(\d+) epoch=([\d.]+)", logs(nid))]


def main():
    os.makedirs(DATA, exist_ok=True)
    dc("down", "-v", "--remove-orphans")
    gen_compose()
    dc("up", "-d", "--build")

    # Aguarda estabilização
    t0 = time.time()
    while time.time() - t0 < 25:
        if any(nid == 3 for nid, _ in announces(3)):
            break
        time.sleep(0.3)
    time.sleep(3.0)

    t_stable = time.time()
    print(f"Cluster estável. Observando por {OBSERVE:.0f}s sem falhas...")
    time.sleep(OBSERVE)
    t_end = time.time()

    result = {
        "observation_seconds": round(t_end - t_stable, 2),
        "expected_leader": 3,
        "nodes": {},
        "elections_after_stable": 0,
        "leaders_agree": True,
    }

    for nid in (1, 2, 3):
        txt = logs(nid)
        # Última adoção/anúncio define o líder atual do nó.
        adopts = re.findall(r"LEADER_ADOPTED node=\d+ leader=(\d+)", txt)
        anns = re.findall(r"LEADER_ANNOUNCE node=(\d+)", txt)
        leader = int(adopts[-1]) if adopts else (int(anns[-1]) if anns else None)
        starts = [float(m.group(1)) for m in
                  re.finditer(r"ELECTION_START node=\d+ epoch=([\d.]+)", txt)]
        spurious = [e for e in starts if e > t_stable]
        result["nodes"][nid] = {
            "leader": leader,
            "elections_total": len(starts),
            "elections_after_stable": len(spurious),
        }
        result["elections_after_stable"] += len(spurious)
        if leader != 3:
            result["leaders_agree"] = False

    result["safety_ok"] = (
        result["leaders_agree"] and result["elections_after_stable"] == 0
    )

    with open(os.path.join(DATA, "safety_results.json"), "w") as fh:
        json.dump(result, fh, indent=2, ensure_ascii=False)

    print(json.dumps(result, indent=2, ensure_ascii=False))
    dc("down", "-v", "--remove-orphans")


if __name__ == "__main__":
    main()

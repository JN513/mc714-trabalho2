#!/usr/bin/env python3
"""Experimentos do terceiro algoritmo (bônus): Two-Phase Commit (2PC).

Cenários:
  A) **throughput/latência**: $N \\in \\{3,4,5\\}$, muitas transações
     concorrentes sem conflito; mede vazão, latência p50/p95 e taxa de commit;
  B) **falha de participante**: derruba um participante e verifica que todas as
     transações abortam (atomicidade) e que os estados convergem;
  C) **falha do coordenador**: o líder cai logo após o PREPARE; os
     participantes ficam bloqueados e o protocolo de terminação cooperativo só
     resolve quando o coordenador reinicia (crash-recovery).

Os resultados são gravados em ``experiments/data/``.
"""

import os
import re
import csv
import json
import time
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SRC = os.path.join(ROOT, "src")
DATA = os.path.join(HERE, "data")
COMPOSE = os.path.join(HERE, "compose.gen.yml")
PROJECT = "mc714_2pc"
LEADER_PORT = 5000
os.makedirs(DATA, exist_ok=True)


# --------------------------------------------------------------------------- #
def gen_compose(n_nodes, crash_after_prepare_on=None, vote_timeout=2.0,
                decision_timeout=3.0):
    lines = ["services:"]
    for i in range(1, n_nodes + 1):
        peers = ",".join(f"{j}:node{j}:5000" for j in range(1, n_nodes + 1) if j != i)
        env = [
            f"      - NODE_ID={i}",
            "      - PORT=5000",
            f"      - PEERS={peers}",
            "      - HEARTBEAT_INTERVAL=1.0",
            "      - HEARTBEAT_TIMEOUT=3.0",
            f"      - VOTE_TIMEOUT={vote_timeout}",
            f"      - DECISION_TIMEOUT={decision_timeout}",
        ]
        if crash_after_prepare_on == i:
            env.append("      - CRASH_AFTER_PREPARE=1")
        lines += [
            f"  node{i}:",
            f"    build: {SRC}",
            f"    container_name: {PROJECT}_node{i}",
            "    ports:",
            f"      - \"{5000 + i}:5000\"",
            "    environment:",
            *env,
            "    networks:",
            "      - ds-net",
        ]
    lines += ["networks:", "  ds-net:", "    driver: bridge"]
    with open(COMPOSE, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def dc(*a):
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", COMPOSE, *a],
        capture_output=True, text=True,
    )


def cname(nid):
    return f"{PROJECT}_node{nid}"


def logs(nid):
    r = subprocess.run(["docker", "logs", cname(nid)], capture_output=True, text=True)
    return r.stdout + r.stderr


def send_msg(nid, payload, port=LEADER_PORT):
    try:
        s = socket.create_connection(("127.0.0.1", 5000 + nid), timeout=3)
        s.sendall(json.dumps(payload).encode())
        s.close()
        return True
    except Exception:
        return False


def wait_for(fn, timeout, poll=0.2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v
        time.sleep(poll)
    return None


def parse_results(nid):
    out = []
    for m in re.finditer(
        r"TXN_RESULT txn=(\S+) decision=(\w+) duration=([\d.]+) epoch=([\d.]+)", logs(nid)
    ):
        out.append((m.group(1), m.group(2), float(m.group(3)), float(m.group(4))))
    return out


def wait_stable_leader(n_nodes, timeout=25.0):
    def ready():
        return f"LEADER_ANNOUNCE node={n_nodes}" in logs(n_nodes)
    if not wait_for(ready, timeout):
        return False
    time.sleep(2.5)
    return True


def snapshot(n_nodes):
    for nid in range(1, n_nodes + 1):
        send_msg(nid, {"type": "SNAPSHOT", "sender_id": 0})
    time.sleep(1.0)
    snaps = {}
    for nid in range(1, n_nodes + 1):
        ms = re.findall(r"SNAPSHOT node=\d+ data=(\{.*?\}) epoch", logs(nid))
        if ms:
            snaps[nid] = json.loads(ms[-1])
    return snaps


def all_consistent(snaps):
    vals = list(snaps.values())
    return len(vals) >= 2 and all(v == vals[0] for v in vals)


# --------------------------------------------------------------------------- #
def scenario_throughput(n_nodes, reps, wf):
    gen_compose(n_nodes, vote_timeout=2.0, decision_timeout=3.0)
    dc("down", "-v", "--remove-orphans")
    dc("up", "-d", "--build")
    if not wait_stable_leader(n_nodes):
        print("  !! não estabilizou")
        dc("down", "-v", "--remove-orphans")
        return
    for rep in range(1, reps + 1):
        m = 60
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(
                lambda i: send_msg(n_nodes, {"type": "TXN", "sender_id": 0,
                                             "ops": [[f"k{rep}_{i}", f"v{i}"]]}),
                range(m),
            ))
        # aguarda todos os resultados no log do coordenador
        wait_for(lambda: len(parse_results(n_nodes)) >= m * rep, timeout=30)
        dt = time.time() - t0
        res = parse_results(n_nodes)[-m:]
        commits = sum(1 for r in res if r[1] == "COMMIT")
        aborts = sum(1 for r in res if r[1] == "ABORT")
        durs = sorted(r[2] for r in res)
        p50 = durs[len(durs) // 2] if durs else float("nan")
        p95 = durs[int(len(durs) * 0.95) - 1] if durs else float("nan")
        wf.writerow([n_nodes, rep, m, commits, aborts, f"{dt:.3f}",
                     f"{m/dt:.2f}", f"{p50:.4f}", f"{p95:.4f}"])
        print(f"    2PC throughput N={n_nodes} rep={rep}: {commits}/{len(res)} commit, "
              f"{m/dt:.1f} txn/s, p50={p50*1000:.1f}ms p95={p95*1000:.1f}ms")
    dc("down", "-v", "--remove-orphans")


def scenario_participant_crash(n_nodes, reps, wf):
    gen_compose(n_nodes, vote_timeout=1.0, decision_timeout=2.0)
    dc("down", "-v", "--remove-orphans")
    dc("up", "-d", "--build")
    if not wait_stable_leader(n_nodes):
        print("  !! não estabilizou")
        dc("down", "-v", "--remove-orphans")
        return
    for rep in range(1, reps + 1):
        subprocess.run(["docker", "kill", cname(2)], capture_output=True)
        time.sleep(0.3)
        before = len(parse_results(n_nodes))
        m = 5
        for i in range(m):
            send_msg(n_nodes, {"type": "TXN", "sender_id": 0,
                               "ops": [[f"pc{rep}_{i}", "v"]]})
        wait_for(lambda: len(parse_results(n_nodes)) >= before + m, timeout=20)
        res = parse_results(n_nodes)[-m:]
        aborts = sum(1 for r in res if r[1] == "ABORT")
        # reinicia o participante e compara estados
        subprocess.run(["docker", "start", cname(2)], capture_output=True)
        time.sleep(3)
        snaps = snapshot(n_nodes)
        atomic = all_consistent(snaps) and all(
            not any(k.startswith(f"pc{rep}_") for k in s) for s in snaps.values()
        )
        wf.writerow(["participant_crash", n_nodes, rep, m, aborts, m - aborts,
                     int(atomic)])
        print(f"    2PC falha de participante rep={rep}: {aborts}/{m} abort, "
              f"consistente={atomic}")
    dc("down", "-v", "--remove-orphans")


def scenario_coordinator_crash(n_nodes, reps, wf):
    for rep in range(1, reps + 1):
        gen_compose(n_nodes, crash_after_prepare_on=n_nodes,
                    vote_timeout=1.0, decision_timeout=2.0)
        dc("down", "-v", "--remove-orphans")
        dc("up", "-d", "--build")
        if not wait_stable_leader(n_nodes):
            print("  !! não estabilizou")
            dc("down", "-v", "--remove-orphans")
            continue

        t_send = time.time()
        send_msg(n_nodes, {"type": "TXN", "sender_id": 0,
                           "ops": [[f"cc{rep}", "v"]]})
        wait_for(lambda: "TXN_COORD_CRASH" in logs(n_nodes), timeout=10)

        # Bloqueio: nenhuma terminação deve ocorrer enquanto o coordenador
        # está morto (filtra terminações posteriores ao envio).
        term_before = wait_for(
            lambda: _terminate_after(n_nodes, t_send), timeout=3.5
        )
        blocked = term_before is None

        t_restart = time.time()
        subprocess.run(["docker", "start", cname(n_nodes)], capture_output=True)
        term = wait_for(lambda: _terminate_after(n_nodes, t_restart), timeout=20)
        resolution = (term[1] - t_send) if term else float("nan")
        decision = term[0] if term else "NONE"
        snaps = snapshot(n_nodes)
        atomic = all_consistent(snaps) and all(
            f"cc{rep}" not in s for s in snaps.values()
        )
        wf.writerow(["coordinator_crash", n_nodes, rep, int(blocked),
                     decision, f"{resolution:.3f}", int(atomic)])
        print(f"    2PC falha do coordenador rep={rep}: bloqueou={blocked}, "
              f"terminação={decision}, resolução={resolution:.2f}s, consistente={atomic}")
        dc("down", "-v", "--remove-orphans")


def _terminate_after(n_nodes, t_after):
    for nid in range(1, n_nodes):
        for m in re.finditer(
            r"TXN_TERMINATE txn=\S+ node=\d+ decision=(\w+) epoch=([\d.]+)", logs(nid)
        ):
            ep = float(m.group(2))
            if ep > t_after:
                return (m.group(1), ep)
    return None


def main():
    reps = 5

    print("=== 2PC: throughput/latência vs. N ===")
    with open(os.path.join(DATA, "2pc_throughput.csv"), "w", newline="") as ft:
        wt = csv.writer(ft)
        wt.writerow(["n_nodes", "rep", "n_txn", "commits", "aborts",
                     "total_time_s", "throughput_txn_s", "p50_s", "p95_s"])
        for N in (3, 4, 5):
            scenario_throughput(N, reps, wt)
            ft.flush()

    print("=== 2PC: falha de participante (atomicidade) ===")
    with open(os.path.join(DATA, "2pc_faults.csv"), "w", newline="") as ff:
        wf = csv.writer(ff)
        wf.writerow(["scenario", "n_nodes", "rep", "n_txn", "aborts", "commits", "atomic"])
        scenario_participant_crash(3, reps, wf)
        ff.flush()

    print("=== 2PC: falha do coordenador (bloqueio + terminação) ===")
    with open(os.path.join(DATA, "2pc_coordinator.csv"), "w", newline="") as fc:
        wc = csv.writer(fc)
        wc.writerow(["scenario", "n_nodes", "rep", "blocked", "decision",
                     "resolution_s", "atomic"])
        scenario_coordinator_crash(3, reps, wc)
        fc.flush()

    print("\nConcluído. Dados em", DATA)


if __name__ == "__main__":
    main()

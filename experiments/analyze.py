#!/usr/bin/env python3
"""Análise dos resultados e geração das figuras e tabelas do relatório.

Lê os CSVs produzidos por ``run_experiments.py`` e ``run_safety.py``, calcula
estatísticas (tendência, dispersão, percentis p50/p95 e IC 95%), gera as
figuras em ``relatorio/figures/`` e as tabelas LaTeX em ``relatorio/tabelas/``.
"""

import os
import csv
import json
import statistics as st
from collections import defaultdict

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
DATA = os.path.join(HERE, "data")
FIGDIR = os.path.join(ROOT, "relatorio", "figures")
TABDIR = os.path.join(ROOT, "relatorio", "tabelas")
os.makedirs(FIGDIR, exist_ok=True)
os.makedirs(TABDIR, exist_ok=True)

T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
       7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179}

plt.rcParams.update({
    "figure.dpi": 140,
    "font.size": 11,
    "axes.grid": True,
    "grid.linestyle": "--",
    "grid.alpha": 0.5,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "font.family": "DejaVu Sans",
})


def br(x, nd=2):
    """Formata número com vírgula decimal (padrão pt-BR)."""
    if x != x:
        return "--"
    return f"{x:.{nd}f}".replace(".", ",")


def ci95(xs):
    xs = [x for x in xs if x == x]
    n = len(xs)
    if n < 2:
        return (xs[0] if xs else float("nan")), 0.0
    m = st.mean(xs)
    s = st.stdev(xs)
    t = T95.get(n - 1, 1.96)
    return m, t * s / np.sqrt(n)


def pct(xs, p):
    xs = sorted(x for x in xs if x == x)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100.0
    lo, hi = int(np.floor(k)), int(np.ceil(k))
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summary(xs):
    xs = [x for x in xs if x == x]
    m, h = ci95(xs)
    return {
        "n": len(xs), "mean": m, "std": (st.stdev(xs) if len(xs) > 1 else 0.0),
        "ci": h, "p50": pct(xs, 50), "p95": pct(xs, 95),
    }


def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return list(csv.DictReader(fh))


def group(rows, key):
    g = defaultdict(list)
    for r in rows:
        g[float(r[key])].append(r)
    return dict(sorted(g.items()))


# --------------------------------------------------------------------------- #
# Figuras
# --------------------------------------------------------------------------- #
def fig_convergencia_timeout(rows):
    byT = group([r for r in rows if r["exp"] == "timeout"], "hb_timeout")
    if not byT:
        return {}
    Ts = sorted(byT)
    conv, det, ele, p95 = [], [], [], []
    for T in Ts:
        rs = byT[T]
        conv.append(summary([float(r["convergence_sec"]) for r in rs]))
        det.append(summary([float(r["detection_sec"]) for r in rs]))
        ele.append(summary([float(r["election_sec"]) for r in rs]))
        p95.append(pct([float(r["convergence_sec"]) for r in rs], 95))

    fig, ax = plt.subplots(figsize=(6.2, 4.0))
    x = np.arange(len(Ts))
    ax.errorbar(x, [c["mean"] for c in conv], yerr=[c["ci"] for c in conv],
                marker="o", color="#c0392b", capsize=4, lw=2,
                label="Reconvergência total (IC 95%)")
    ax.errorbar(x, [c["mean"] for c in det], yerr=[c["ci"] for c in det],
                marker="s", color="#2980b9", capsize=4, lw=1.6, ls="--",
                label="Detecção (timeout)")
    ax.errorbar(x, [c["mean"] for c in ele], yerr=[c["ci"] for c in ele],
                marker="^", color="#27ae60", capsize=4, lw=1.6, ls=":",
                label="Eleição (resposta + anúncio)")
    ax.plot(x, p95, marker="x", color="#8e44ad", ls="none", label="p95 da reconvergência")
    ax.plot(x, [T + 2.0 for T in Ts], color="black", lw=1.2, ls="-.",
            label=r"Modelo $T_{hb} + T_{ele}$")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{T:g}" for T in Ts])
    ax.set_xlabel("Limiar de heartbeat $T_{hb}$ (s)")
    ax.set_ylabel("Tempo (s)")
    ax.set_title("Reconvergência vs. limiar de heartbeat ($N=3$)")
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig1_convergencia_timeout.png"))
    plt.close(fig)
    return {str(T): {"conv": conv[i], "det": det[i], "ele": ele[i], "p95": p95[i]}
            for i, T in enumerate(Ts)}


def fig_convergencia_nos(rows):
    byN = group([r for r in rows if r["exp"] == "nodes"], "n_nodes")
    Ns = sorted(byN)
    if not Ns:
        return {}
    conv, msgs = [], []
    for N in Ns:
        rs = byN[N]
        conv.append(summary([float(r["convergence_sec"]) for r in rs]))
        msgs.append(summary([float(r["election_messages"]) for r in rs]))

    fig, ax1 = plt.subplots(figsize=(6.2, 4.0))
    x = np.arange(len(Ns))
    ax1.errorbar(x, [c["mean"] for c in conv], yerr=[c["ci"] for c in conv],
                 marker="o", color="#c0392b", capsize=4, lw=2,
                 label="Reconvergência (IC 95%)")
    ax1.plot(x, [c["p95"] for c in conv], marker="x", color="#8e44ad", ls="none", label="p95")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{int(N)}" for N in Ns])
    ax1.set_xlabel("Número de nós $N$")
    ax1.set_ylabel("Tempo de reconvergência (s)", color="#c0392b")
    ax1.tick_params(axis="y", labelcolor="#c0392b")
    ax2 = ax1.twinx()
    ax2.plot(x, [m["mean"] for m in msgs], marker="D", color="#2980b9", lw=1.8,
             label="Mensagens de eleição")
    ax2.set_ylabel("Mensagens por eleição", color="#2980b9")
    ax2.tick_params(axis="y", labelcolor="#2980b9")
    ax2.grid(False)
    ax1.set_title("Reconvergência e custo de mensagens vs. $N$ ($T_{hb}=3$ s)")
    l1, n1 = ax1.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, n1 + n2, fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig2_convergencia_nos.png"))
    plt.close(fig)
    return {str(int(N)): {"conv": conv[i], "msgs": msgs[i]} for i, N in enumerate(Ns)}


def fig_mensagens(rows):
    byN = group([r for r in rows if r["exp"] == "nodes"], "n_nodes")
    Ns = sorted(byN)
    if not Ns:
        return
    msgs = [st.mean([float(r["election_messages"]) for r in byN[N]]) for N in Ns]
    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    ax.plot(Ns, msgs, marker="D", color="#2980b9", lw=2, label="Medido")
    xs = np.linspace(min(Ns), max(Ns), 50)
    ax.plot(xs, [msgs[-1] * (x / max(Ns)) ** 2 for x in xs], ls="--",
            color="#7f8c8d", label=r"Referência $O(N^2)$")
    ax.set_xlabel("Número de nós $N$")
    ax.set_ylabel("Mensagens de eleição (média)")
    ax.set_title("Complexidade de mensagens do Algoritmo do Valentão")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig3_mensagens_complexidade.png"))
    plt.close(fig)


def fig_recuperacao(rrows):
    byN = group([r for r in rrows if r["exp"] == "nodes"], "n_nodes")
    Ns = sorted(byN)
    if not Ns:
        return {}
    data = [[float(r["recovery_sec"]) for r in byN[N]] for N in Ns]
    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    bp = ax.boxplot(data, tick_labels=[f"N={int(N)}" for N in Ns],
                    patch_artist=True, showmeans=True)
    for patch in bp["boxes"]:
        patch.set_facecolor("#aed6f1")
    ax.set_ylabel("Tempo de recuperação (s)")
    ax.set_title("Crash-recovery: tempo até o maior ID reassumir")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig4_recuperacao.png"))
    plt.close(fig)
    return {str(int(N)): summary(data[i]) for i, N in enumerate(Ns)}


def fig_timeline():
    fig, ax = plt.subplots(figsize=(6.4, 3.4))
    for t in np.arange(0, 5.0, 1.0):
        ax.axvline(t, color="#3498db", alpha=0.25, lw=6)
    ax.text(0.1, 3.35, "Heartbeats do líder (node3)", color="#2980b9", fontsize=9)
    kill, detect, announce, recover = 4.0, 6.5, 8.5, 10.6
    ax.axvspan(0, kill, color="#eafaf1", alpha=0.6)
    ax.axvspan(kill, announce, color="#fdecea", alpha=0.7)
    ax.axvspan(announce, recover, color="#fef9e7", alpha=0.7)
    ax.axvspan(recover, recover + 2, color="#eafaf1", alpha=0.6)
    ax.annotate("", xy=(announce, 2.0), xytext=(kill, 2.0),
                arrowprops=dict(arrowstyle="<->", color="#c0392b", lw=1.6))
    ax.text((kill + announce) / 2, 2.1, "reconvergência", ha="center",
            color="#c0392b", fontsize=9)
    ax.annotate("", xy=(recover + 2.0, 1.2), xytext=(recover, 1.2),
                arrowprops=dict(arrowstyle="<->", color="#7d6608", lw=1.6))
    ax.text(recover + 1.0, 1.3, "recuperação", ha="center", color="#7d6608", fontsize=9)
    marks = [(kill, "#c0392b", "X", "docker kill\n(node3)"),
             (detect, "#e67e22", "v", "timeout\ndetectado"),
             (announce, "#27ae60", "o", "node2\nassume"),
             (recover, "#8e44ad", "D", "node3\nreinicia")]
    for x, c, m, txt in marks:
        ax.scatter([x], [0.6], color=c, marker=m, s=80, zorder=5)
        ax.text(x, 0.2, txt, ha="center", color=c, fontsize=8)
    ax.set_ylim(0, 3.8)
    ax.set_xlim(0, recover + 2.2)
    ax.set_yticks([])
    ax.set_xlabel("Tempo (s)")
    ax.set_title("Ciclo típico: falha crash-stop seguida de recuperação")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig5_timeline.png"))
    plt.close(fig)


def fig_arquitetura():
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    ax.set_xlim(0, 10); ax.set_ylim(0, 6); ax.axis("off")
    ax.add_patch(plt.Rectangle((0.3, 0.3), 9.4, 5.4, fill=False, ls="--", ec="#7f8c8d"))
    ax.text(0.5, 5.45, "docker compose  •  rede bridge ds-net", color="#7f8c8d", fontsize=9)
    pos = {1: (2.0, 4.0), 2: (5.0, 4.0), 3: (8.0, 4.0)}
    labels = {1: "node1\nseguidor", 2: "node2\nseguidor", 3: "node3\nLÍDER (maior ID)"}
    colors = {1: "#aed6f1", 2: "#aed6f1", 3: "#f9e79f"}
    for i, (x, y) in pos.items():
        ax.add_patch(plt.Circle((x, y), 0.85, fc=colors[i], ec="#2c3e50", lw=1.5))
        ax.text(x, y, labels[i], ha="center", va="center", fontsize=8)
    for i in (1, 2):
        ax.annotate("", xy=(pos[i][0], 3.1), xytext=(pos[3][0], 3.1),
                    arrowprops=dict(arrowstyle="->", color="#2980b9", lw=1.4))
    ax.text(5.0, 2.75, "HEARTBEAT (periódico, do líder para os seguidores)",
            ha="center", color="#2980b9", fontsize=8)
    ax.annotate("", xy=(pos[3][0], 4.9), xytext=(pos[2][0], 4.9),
                arrowprops=dict(arrowstyle="->", color="#c0392b", lw=1.4))
    ax.text(6.5, 5.15, "ELECTION", color="#c0392b", fontsize=8, ha="center")
    ax.annotate("", xy=(pos[2][0], 4.9), xytext=(pos[1][0], 4.9),
                arrowprops=dict(arrowstyle="<-", color="#27ae60", lw=1.4))
    ax.text(3.5, 5.15, "ANSWER", color="#1e8449", fontsize=8, ha="center")
    ax.text(5.0, 1.0, "Camada: TCP + JSON (asyncio)   |   Algoritmos: Bully + Heartbeat",
            ha="center", fontsize=8.5, style="italic")
    ax.set_title("Arquitetura da solução")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig6_arquitetura.png"))
    plt.close(fig)


# --------------------------------------------------------------------------- #
# Tabelas LaTeX
# --------------------------------------------------------------------------- #
def write_tables(stats):
    tt = stats.get("timeout", {})
    if tt:
        lines = [r"\begin{tabular}{ccccc}", r"\toprule",
                 r"$T_{hb}$ (s) & Reconvergência (s) & Detecção (s) & Eleição (s) & p95 (s) \\",
                 r"\midrule"]
        for T, s in tt.items():
            c, d, e = s["conv"], s["det"], s["ele"]
            lines.append(
                f"{br(float(T),0)} & {br(c['mean'])} $\\pm$ {br(c['ci'])} & "
                f"{br(d['mean'])} & {br(e['mean'])} & {br(s['p95'])} \\\\"
            )
        lines += [r"\bottomrule", r"\end{tabular}"]
        with open(os.path.join(TABDIR, "tab_timeout.tex"), "w") as fh:
            fh.write("\n".join(lines))

    nn = stats.get("nodes", {})
    if nn:
        lines = [r"\begin{tabular}{cccc}", r"\toprule",
                 r"$N$ & Reconvergência (s) & p95 (s) & Mensagens \\", r"\midrule"]
        for N, s in nn.items():
            c, m = s["conv"], s["msgs"]
            lines.append(
                f"{N} & {br(c['mean'])} $\\pm$ {br(c['ci'])} & {br(c['p95'])} & "
                f"{br(m['mean'],1)} \\\\"
            )
        lines += [r"\bottomrule", r"\end{tabular}"]
        with open(os.path.join(TABDIR, "tab_nos.tex"), "w") as fh:
            fh.write("\n".join(lines))

    rr = stats.get("recovery", {})
    if rr:
        lines = [r"\begin{tabular}{ccc}", r"\toprule",
                 r"$N$ & Recuperação (s) & p95 (s) \\", r"\midrule"]
        for N, s in rr.items():
            lines.append(f"{N} & {br(s['mean'])} $\\pm$ {br(s['ci'])} & {br(s['p95'])} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        with open(os.path.join(TABDIR, "tab_recuperacao.tex"), "w") as fh:
            fh.write("\n".join(lines))


# --------------------------------------------------------------------------- #
# Figuras e tabelas do 2PC (bônus)
# --------------------------------------------------------------------------- #
def fig_2pc_throughput():
    rows = load(os.path.join(DATA, "2pc_throughput.csv"))
    if not rows:
        return {}
    byN = defaultdict(list)
    for r in rows:
        byN[int(r["n_nodes"])].append(r)
    Ns = sorted(byN)
    thr, p50, p95, commit = [], [], [], []
    for N in Ns:
        rs = byN[N]
        # Mediana entre réplicas (robusta a picos ocasionais de backlog TCP).
        thr.append(st.median(float(r["throughput_txn_s"]) for r in rs))
        p50.append(st.median(float(r["p50_s"]) for r in rs) * 1000)
        p95.append(st.median(float(r["p95_s"]) for r in rs) * 1000)
        commit.append(st.median(float(r["commits"]) / float(r["n_txn"]) for r in rs) * 100)

    fig, ax1 = plt.subplots(figsize=(6.2, 4.0))
    x = np.arange(len(Ns))
    ax1.bar(x - 0.0, thr, width=0.5, color="#5dade2", label="Vazão (txn/s)")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{N}" for N in Ns])
    ax1.set_xlabel("Número de nós $N$")
    ax1.set_ylabel("Vazão (txn/s)", color="#1f618d")
    ax1.tick_params(axis="y", labelcolor="#1f618d")
    ax2 = ax1.twinx()
    ax2.plot(x, p50, marker="o", color="#c0392b", lw=2, label="Latência p50")
    ax2.plot(x, p95, marker="s", color="#8e44ad", lw=1.6, ls="--", label="Latência p95")
    ax2.set_ylabel("Latência (ms)", color="#c0392b")
    ax2.tick_params(axis="y", labelcolor="#c0392b")
    ax2.grid(False)
    ax1.set_title("2PC: vazão e latência vs. $N$")
    l1, n1 = ax1.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, n1 + n2, fontsize=8, loc="upper right")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig7_2pc_throughput.png"))
    plt.close(fig)
    return {str(N): {"throughput": thr[i], "p50_ms": p50[i], "p95_ms": p95[i],
                     "commit_pct": commit[i]}
            for i, N in enumerate(Ns)}


def fig_2pc_faults():
    pcrash = load(os.path.join(DATA, "2pc_faults.csv"))
    ccrash = load(os.path.join(DATA, "2pc_coordinator.csv"))
    tp = load(os.path.join(DATA, "2pc_throughput.csv"))
    if not tp and not pcrash and not ccrash:
        return {}

    def rate(rows, key_c, key_t):
        c = sum(float(r[key_c]) for r in rows)
        t = sum(float(r[key_t]) for r in rows)
        return 100.0 * c / t if t else 0.0

    normal_commit = rate(tp, "commits", "n_txn")
    p_abort = rate(pcrash, "aborts", "n_txn")
    p_atomic = all(int(r["atomic"]) == 1 for r in pcrash) if pcrash else True
    c_atomic = all(int(r["atomic"]) == 1 for r in ccrash) if ccrash else True
    c_blocked = all(int(r["blocked"]) == 1 for r in ccrash) if ccrash else False
    c_res = st.mean(float(r["resolution_s"]) for r in ccrash) if ccrash else float("nan")

    labels = ["Normal", "Falha de\nparticipante", "Falha do\ncoordenador"]
    commits = [normal_commit, 100 - p_abort, 0.0]
    aborts = [100 - normal_commit, p_abort, 100.0]

    fig, ax = plt.subplots(figsize=(6.2, 4.0))
    x = np.arange(3)
    ax.bar(x, commits, color="#27ae60", label="% COMMIT")
    ax.bar(x, aborts, bottom=commits, color="#c0392b", label="% ABORT")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Transações (%)")
    ax.set_ylim(0, 118)
    ax.set_title("2PC: resultados por cenário (atomicidade)")
    ax.legend(fontsize=8, loc="upper center", ncol=2)
    notes = [f"atômico: {'sim' if p_atomic else 'não'}",
             f"atômico: {'sim' if c_atomic else 'não'}\nbloqueio + terminação (~{c_res:.1f}s)"]
    ax.annotate(notes[0], xy=(1, 104), ha="center", fontsize=8, color="#1e8449")
    ax.annotate(notes[1], xy=(2, 104), ha="center", fontsize=8, color="#7d6608")
    fig.tight_layout()
    fig.savefig(os.path.join(FIGDIR, "fig8_2pc_faults.png"))
    plt.close(fig)
    return {"normal_commit_pct": normal_commit, "participant_abort_pct": p_abort,
            "participant_atomic": p_atomic, "coordinator_atomic": c_atomic,
            "coordinator_blocked": c_blocked, "coordinator_resolution_s": c_res}


def write_tables_2pc(stats):
    tp = stats.get("2pc_throughput", {})
    if tp:
        lines = [r"\begin{tabular}{cccc}", r"\toprule",
                 r"$N$ & Vazão (txn/s) & p50 (ms) & p95 (ms) \\", r"\midrule"]
        for N, s in tp.items():
            lines.append(f"{N} & {br(s['throughput'],1)} & {br(s['p50_ms'],1)} & "
                         f"{br(s['p95_ms'],1)} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        with open(os.path.join(TABDIR, "tab_2pc_throughput.tex"), "w") as fh:
            fh.write("\n".join(lines))

    ff = stats.get("2pc_faults", {})
    if ff:
        lines = [r"\begin{tabular}{lccc}", r"\toprule",
                 r"Cenário & Commit (\%) & Abort (\%) & Atômico \\", r"\midrule"]
        lines.append(f"Normal & {br(ff['normal_commit_pct'],1)} & "
                     f"{br(100-ff['normal_commit_pct'],1)} & sim \\\\")
        lines.append(f"Falha de participante & {br(100-ff['participant_abort_pct'],1)} & "
                     f"{br(ff['participant_abort_pct'],1)} & "
                     f"{'sim' if ff['participant_atomic'] else 'não'} \\\\")
        lines.append(f"Falha do coordenador & 0,0 & 100,0 & "
                     f"{'sim' if ff['coordinator_atomic'] else 'não'} \\\\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        with open(os.path.join(TABDIR, "tab_2pc_faults.tex"), "w") as fh:
            fh.write("\n".join(lines))


def main():
    rows = load(os.path.join(DATA, "failover_results.csv"))
    rrows = load(os.path.join(DATA, "recovery_results.csv"))

    stats = {}
    stats["timeout"] = fig_convergencia_timeout(rows)
    stats["nodes"] = fig_convergencia_nos(rows)
    fig_mensagens(rows)
    stats["recovery"] = fig_recuperacao(rrows)
    fig_timeline()
    fig_arquitetura()
    write_tables(stats)

    # 2PC (bônus)
    stats["2pc_throughput"] = fig_2pc_throughput()
    stats["2pc_faults"] = fig_2pc_faults()
    write_tables_2pc(stats)

    with open(os.path.join(DATA, "stats.json"), "w") as fh:
        json.dump(stats, fh, indent=2, ensure_ascii=False)
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    print("\nFiguras em", FIGDIR, "| Tabelas em", TABDIR)


if __name__ == "__main__":
    main()

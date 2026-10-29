#!/usr/bin/env python3
"""gate 任务的工作区生成器（固定种子、确定性、答案可自动校验）。

任务形态（"三段锁"）
  1. `work/start.txt` 给出一个 pointer（随机 hex）。
  2. `work/vault/<sha256(pointer)[:16]>.txt` 给出 code（C1）与 route（slug）。
  3. `work/routes.json` 把 slug 映射到 bin 名与 salt。
  4. `work/bins/<bin>.txt` 给出 check（C2）。
  5. 最终答案 = `<C1>:<C2>:<sha256(C1:C2:salt)[:12]>`，写进工作区根的 `answer.txt`。

为什么这样设计
  - 每一步的输入都是上一步输出里的随机值，模型猜不到，必须顺序做；第 5 步要算 sha256，
    模型只能靠工具（bash）算，天然形成 ≥4 个顺序工具调用。
  - 文件都很小（几十字节到 1 KB），单次试验的 token 消耗低。
  - 过程一眼能看懂（每步只打印一个短 token），适合录屏。

用法
  python3 gen_task.py --workspace DIR [--seed 20261026]   # 生成工作区
  python3 gen_task.py --workspace DIR --print-prompt      # 渲染提示词
  python3 gen_task.py --workspace DIR --check             # 校验 answer.txt（参考）

副作用
  只写 --workspace 下的文件（会先清空该目录里的 work/、answer.txt 与提示词副本）。
  不联网、不读凭据。生成是纯确定性的：同一 seed 同一文件树、同一字节。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sys
from pathlib import Path

DEFAULT_SEED = 20261026
PROMPT_PATH = Path(__file__).resolve().parent / "prompt.md"
WORK = "work"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def hex_token(rng: random.Random, length: int) -> str:
    return "".join(rng.choice("0123456789abcdef") for _ in range(length))


def plan(seed: int = DEFAULT_SEED) -> dict:
    """生成整个任务所需的随机值（不落盘，便于生成器与校验共用）。"""
    rng = random.Random(seed)
    pointer = hex_token(rng, 24)
    vault_name = sha(pointer)[:16]
    code = hex_token(rng, 12)
    salt = hex_token(rng, 8)
    check = hex_token(rng, 12)
    # bin 名与 lane 名必须全局唯一：否则诱饵文件会覆盖目标文件，链就断了。
    bins = set()
    slugs = set()

    def unique_bin() -> str:
        while True:
            name = "gate-" + str(rng.randint(10, 99))
            if name not in bins:
                bins.add(name)
                return name

    def unique_slug() -> str:
        while True:
            name = f"lane-{hex_token(rng, 4)}"
            if name not in slugs:
                slugs.add(name)
                return name

    target_slug = unique_slug()
    target_bin = unique_bin()
    # 诱饵：vault 里另有 6 个文件、routes 里另有 5 条 lane，避免“唯一文件=答案”
    decoys = []
    for _ in range(6):
        decoys.append(
            {
                "name": sha(hex_token(rng, 24))[:16],
                "pointer": hex_token(rng, 24),
                "code": hex_token(rng, 12),
                "slug": unique_slug(),
            }
        )
    extra_lanes = [
        {"slug": unique_slug(), "bin": unique_bin(), "salt": hex_token(rng, 8)} for _ in range(5)
    ]
    answer = f"{code}:{check}:{sha(f'{code}:{check}:{salt}')[:12]}"
    return {
        "seed": seed,
        "pointer": pointer,
        "vault_name": vault_name,
        "code": code,
        "salt": salt,
        "check": check,
        "slug": target_slug,
        "bin": target_bin,
        "decoys": decoys,
        "extra_lanes": extra_lanes,
        "answer": answer,
    }


def generate(workspace: Path, seed: int = DEFAULT_SEED) -> dict:
    """按 plan(seed) 写出工作区；返回计划（含答案，供假上游与校验使用）。"""
    spec = plan(seed)
    work = workspace / WORK
    if work.exists():
        shutil.rmtree(work)
    (work / "vault").mkdir(parents=True)
    (work / "bins").mkdir(parents=True)
    (workspace / "answer.txt").unlink(missing_ok=True)

    (work / "start.txt").write_text(f"pointer: {spec['pointer']}\n", encoding="utf-8")

    (work / "vault" / f"{spec['vault_name']}.txt").write_text(
        f"code: {spec['code']}\nroute: {spec['slug']}\n", encoding="utf-8"
    )
    for decoy in spec["decoys"]:
        (work / "vault" / f"{decoy['name']}.txt").write_text(
            f"code: {decoy['code']}\nroute: {decoy['slug']}\n", encoding="utf-8"
        )

    routes = {spec["slug"]: {"bin": spec["bin"], "salt": spec["salt"]}}
    for lane in spec["extra_lanes"]:
        routes[lane["slug"]] = {"bin": lane["bin"], "salt": lane["salt"]}
    (work / "routes.json").write_text(json.dumps(routes, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    (work / "bins" / f"{spec['bin']}.txt").write_text(f"check: {spec['check']}\n", encoding="utf-8")
    for lane in spec["extra_lanes"]:
        (work / "bins" / f"{lane['bin']}.txt").write_text(f"check: {hex_token(random.Random(lane['bin']), 12)}\n", encoding="utf-8")

    # 提示词副本：录屏时工作区里也看得到任务说明，且与 gate.py 发出的文本同源。
    (workspace / "TASK.md").write_text(render_prompt(workspace), encoding="utf-8")
    return spec


def expected_answer(workspace: Path) -> str:
    """从工作区现读现算答案（不依赖 plan），保证校验与工作区一致。"""
    start = (workspace / WORK / "start.txt").read_text(encoding="utf-8")
    pointer = start.split("pointer:", 1)[1].strip()
    vault = (workspace / WORK / "vault" / f"{sha(pointer)[:16]}.txt").read_text(encoding="utf-8")
    fields = dict(
        line.split(":", 1) for line in vault.strip().splitlines() if ":" in line
    )
    code, slug = fields["code"].strip(), fields["route"].strip()
    routes = json.loads((workspace / WORK / "routes.json").read_text(encoding="utf-8"))
    lane = routes[slug]
    check = (workspace / WORK / "bins" / f"{lane['bin']}.txt").read_text(encoding="utf-8").split("check:", 1)[1].strip()
    return f"{code}:{check}:{sha(f'{code}:{check}:{lane['salt']}')[:12]}"


def shell_steps(workspace: Path) -> list[str]:
    """脚本化假模型要跑的命令序列（与真人/真模型的做法一致，每步只依赖上一步输出）。"""
    start = (workspace / WORK / "start.txt").read_text(encoding="utf-8")
    pointer = start.split("pointer:", 1)[1].strip()
    vault_rel = f"{WORK}/vault/{sha(pointer)[:16]}.txt"
    vault = (workspace / vault_rel).read_text(encoding="utf-8")
    fields = dict(line.split(":", 1) for line in vault.strip().splitlines() if ":" in line)
    code, slug = fields["code"].strip(), fields["route"].strip()
    routes = json.loads((workspace / WORK / "routes.json").read_text(encoding="utf-8"))
    lane = routes[slug]
    check = (workspace / WORK / "bins" / f"{lane['bin']}.txt").read_text(encoding="utf-8").split("check:", 1)[1].strip()
    # 第 5 步用 shell 现算哈希（不把答案写死），这样演练同时也是对「算法可复现」的检查：
    # 若 shell 的结果与 expected_answer() 的 Python 结果不同，answer.ok 会变 false。
    return [
        f"cat {WORK}/start.txt",
        f"cat {vault_rel}",
        f"cat {WORK}/routes.json",
        f"cat {WORK}/bins/{lane['bin']}.txt",
        (
            f"c1={code}; c2={check}; s={lane['salt']}; "
            "printf '%s:%s:%s\\n' \"$c1\" \"$c2\" "
            "\"$(printf '%s' \"$c1:$c2:$s\" | sha256sum | cut -c1-12)\" > answer.txt"
        ),
    ]


def render_prompt(workspace: Path) -> str:
    return PROMPT_PATH.read_text(encoding="utf-8").replace("{{WORKSPACE}}", str(workspace))


def check(workspace: Path) -> dict:
    """校验 answer.txt：只比较去掉行尾空白后的单行内容。"""
    expected = expected_answer(workspace)
    path = workspace / "answer.txt"
    if not path.is_file():
        return {"ok": False, "reason": "answer.txt 不存在", "expected_sha256": sha(expected)}
    raw = path.read_text(encoding="utf-8")
    got = raw.strip()
    return {
        "ok": got == expected,
        "reason": None if got == expected else "内容不匹配",
        "expected_sha256": sha(expected),
        "got_sha256": sha(got),
        "got_repr": repr(raw[:200]),
        "got_lines": len([line for line in raw.splitlines() if line.strip()]),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="生成 gate 任务工作区")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--print-prompt", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    args.workspace.mkdir(parents=True, exist_ok=True)
    if args.print_prompt:
        print(render_prompt(args.workspace))
        return 0
    if args.check:
        print(json.dumps(check(args.workspace), ensure_ascii=False, indent=2))
        return 0
    spec = generate(args.workspace, args.seed)
    print(
        json.dumps(
            {
                "workspace": str(args.workspace),
                "seed": spec["seed"],
                "answer_sha256": sha(spec["answer"]),
                "files": sorted(str(path.relative_to(args.workspace)) for path in args.workspace.rglob("*") if path.is_file()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

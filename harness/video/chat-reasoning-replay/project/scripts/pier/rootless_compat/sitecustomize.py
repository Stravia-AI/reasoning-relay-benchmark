"""rootless podman 兼容垫片：只修正 Pier 对「容器内 uid 等于宿主 uid」的假设。

用途
- Pier 的 DockerEnvironment 在把 bind mount 里的日志目录交还宿主前，会执行
  `chown -R <os.getuid()>:<os.getgid()> /logs`（`_chown_to_host_user`），
  假定容器内 root 就是宿主用户。rootless podman 下容器 root 映射到宿主用户，
  而容器内 uid 1000 映射到 subuid（本机 100999），于是这条 chown 会把
  trial 目录改成宿主不可写的 uid，后续 `podman cp` 与 manifest 写入全部失败。
- 因此在本进程里让 getuid/getgid 返回 0，使 Pier 发出 `chown -R 0:0`，
  在 rootless podman 下等价于「交还给宿主用户」，语义正确。

输入/副作用
- 通过 PYTHONPATH 只对 pier 进程生效（见 run-pier.sh）；不落盘、不联网。
- 仅在 PIER_ROOTLESS_COMPAT=1 时打补丁，避免影响其他 Python 进程。
"""

import os

if os.environ.get("PIER_ROOTLESS_COMPAT") == "1":
    os.getuid = lambda: 0  # type: ignore[assignment]
    os.getgid = lambda: 0  # type: ignore[assignment]

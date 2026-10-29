// 容器侧单目标转发：127.0.0.1:<port> → 宿主挂进来的 Unix socket。
//
// 为什么需要它：agent 容器是 network_mode: none（无外部接口、无 DNS），DSH 只能连
// 容器回环。宿主把本试验专属的 socket 挂在 /logs/agent/ 里，这里把回环端口上的 TCP
// 连接逐字节接到那个 socket 上，DSH 看到一个普通的 http://127.0.0.1:<port>/v1。
//
// 输入（命令行）：<port> <unix-socket-path>
// 副作用：只在容器回环 127.0.0.1 上监听给定端口，不解析域名、不连接其它地址。
// 异常：socket 不存在/对端拒绝时直接销毁该连接；进程本身保持运行，等下一次连接。

const net = require("node:net");

const [portArg, socketPath] = process.argv.slice(2);
const port = Number(portArg);
if (!port || !socketPath) {
  console.error("usage: upstream-relay.js <port> <unix-socket-path>");
  process.exit(2);
}

const server = net.createServer((client) => {
  client.setNoDelay(true);
  const upstream = net.connect(socketPath);
  let closed = false;
  const destroy = () => {
    if (closed) return;
    closed = true;
    client.destroy();
    upstream.destroy();
  };
  client.on("error", destroy);
  upstream.on("error", destroy);
  client.on("close", () => {
    if (!closed) {
      closed = true;
      upstream.destroy();
    }
  });
  upstream.on("close", () => {
    if (!closed) {
      closed = true;
      client.destroy();
    }
  });
  // 单向 EOF 时用 end() 传递半关闭，避免截断仍在传输的响应体。
  client.pipe(upstream);
  upstream.pipe(client);
  client.on("end", () => upstream.end());
  upstream.on("end", () => client.end());
});

server.on("error", (error) => {
  console.error(`upstream-relay: ${error.message}`);
  process.exit(1);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`upstream-relay: 127.0.0.1:${port} -> ${socketPath}`);
});

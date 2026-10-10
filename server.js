// Zero-dependency web server for the dashboard (Node 18+).
const http = require("http");
const fs = require("fs");
const path = require("path");
const { getDashboardData } = require("./api/dashboard");

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, "public");
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

function sendJson(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(body));
}

const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, "http://localhost");

  if (pathname === "/api/health") return sendJson(res, 200, { status: "ok", time: new Date().toISOString() });

  if (pathname === "/api/dashboard") {
    try {
      return sendJson(res, 200, await getDashboardData());
    } catch (error) {
      console.error("[dashboard]", error.message);
      return sendJson(res, 502, { error: error.message });
    }
  }

  const requested = path.normalize(path.join(PUBLIC_DIR, pathname === "/" ? "index.html" : pathname));
  if (!requested.startsWith(PUBLIC_DIR + path.sep)) {
    res.writeHead(403).end("Forbidden");
    return;
  }
  fs.readFile(requested, (error, content) => {
    if (error) return res.writeHead(404, { "Content-Type": "text/plain" }).end("Not found");
    res.writeHead(200, { "Content-Type": TYPES[path.extname(requested).toLowerCase()] || "application/octet-stream" });
    res.end(content);
  });
});

server.listen(PORT, () => {
  console.log(`Dashboard  http://localhost:${PORT}`);
  console.log(`API        http://localhost:${PORT}/api/dashboard`);
  if (process.env.DEMO_MODE === "true") console.log("DEMO_MODE is on: serving sample data");
});

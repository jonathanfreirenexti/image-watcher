import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { Client } from 'ssh2';

const SSH_HOST = process.env.SSH_HOST;
const SSH_PORT = parseInt(process.env.SSH_PORT || '22', 10);
const SSH_USER = process.env.SSH_USER;
const SSH_PASSWORD = process.env.SSH_PASSWORD;
// Private key must be stored as base64 in env: `base64 -w 0 ~/.ssh/id_rsa`
const SSH_PRIVATE_KEY = process.env.SSH_PRIVATE_KEY;
const ALLOWED_BASE_PATH = process.env.ALLOWED_BASE_PATH || '/';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '2000', 10);
const PORT = parseInt(process.env.PORT || '3001', 10);
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']);

const MIME = {
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png',
  gif: 'image/gif', webp: 'image/webp', bmp: 'image/bmp', svg: 'image/svg+xml',
};

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function isRegularFile(attrs) {
  // POSIX mode bit 0o100000 = regular file
  return attrs && (attrs.mode & 0o170000) === 0o100000;
}

function safePath(requestedPath) {
  const norm = requestedPath.replace(/\.\./g, '').replace(/\/+/g, '/');
  if (!norm.startsWith(ALLOWED_BASE_PATH)) return null;
  return norm;
}

function getExt(filename) {
  const idx = filename.lastIndexOf('.');
  return idx >= 0 ? filename.slice(idx).toLowerCase() : '';
}

function readFileAsBase64(sftp, filePath) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const stream = sftp.createReadStream(filePath);
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('base64')));
    stream.on('error', reject);
  });
}

const server = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('image-watcher ok');
});

const wss = new WebSocketServer({
  server,
  verifyClient: ({ origin }) => ALLOWED_ORIGIN === '*' || origin === ALLOWED_ORIGIN,
});

wss.on('connection', (ws) => {
  let ssh = null;
  let sftp = null;
  let timer = null;
  let seenFiles = new Set();
  let watching = false;

  function cleanup() {
    watching = false;
    if (timer) { clearInterval(timer); timer = null; }
    if (sftp) { try { sftp.end(); } catch (_) {} sftp = null; }
    if (ssh) { try { ssh.end(); } catch (_) {} ssh = null; }
  }

  async function checkFolder(watchPath) {
    if (!sftp || !watching) return;
    sftp.readdir(watchPath, async (err, list) => {
      if (err) { send(ws, { type: 'error', message: `readdir: ${err.message}` }); return; }

      const newImages = list.filter((entry) => {
        const ext = getExt(entry.filename);
        return IMAGE_EXTS.has(ext) && isRegularFile(entry.attrs) && !seenFiles.has(entry.filename);
      });

      for (const entry of newImages) {
        seenFiles.add(entry.filename);
        const filePath = `${watchPath}/${entry.filename}`;
        try {
          // Small delay — let external service finish writing the file
          await new Promise((r) => setTimeout(r, 500));
          const base64 = await readFileAsBase64(sftp, filePath);
          const rawExt = getExt(entry.filename).slice(1);
          const mimeType = MIME[rawExt] ?? 'image/jpeg';
          send(ws, {
            type: 'image',
            filename: entry.filename,
            dataUrl: `data:${mimeType};base64,${base64}`,
            timestamp: Date.now(),
          });
        } catch (readErr) {
          send(ws, { type: 'warn', message: `Could not read ${entry.filename}: ${readErr.message}` });
        }
      }
    });
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { send(ws, { type: 'error', message: 'Invalid JSON' }); return; }

    if (msg.action === 'stop') {
      cleanup();
      send(ws, { type: 'stopped' });
      return;
    }

    if (msg.action === 'start') {
      if (watching) { send(ws, { type: 'error', message: 'Already watching' }); return; }

      const watchPath = safePath(msg.path || ALLOWED_BASE_PATH);
      if (!watchPath) {
        send(ws, { type: 'error', message: 'Invalid or disallowed path' });
        return;
      }

      if (!SSH_HOST || !SSH_USER || (!SSH_PASSWORD && !SSH_PRIVATE_KEY)) {
        send(ws, { type: 'error', message: 'SSH credentials not configured on server' });
        return;
      }

      watching = true;
      seenFiles = new Set();
      ssh = new Client();

      const sshConfig = { host: SSH_HOST, port: SSH_PORT, username: SSH_USER };
      if (SSH_PRIVATE_KEY) {
        sshConfig.privateKey = Buffer.from(SSH_PRIVATE_KEY, 'base64').toString('utf-8');
      } else {
        sshConfig.password = SSH_PASSWORD;
      }

      ssh.on('ready', () => {
        send(ws, { type: 'connected', message: `SSH connected. Watching ${watchPath}` });
        ssh.sftp((err, _sftp) => {
          if (err) {
            send(ws, { type: 'error', message: `SFTP init failed: ${err.message}` });
            cleanup(); return;
          }
          sftp = _sftp;
          checkFolder(watchPath);
          timer = setInterval(() => checkFolder(watchPath), POLL_INTERVAL_MS);
        });
      });

      ssh.on('error', (err) => {
        send(ws, { type: 'error', message: `SSH error: ${err.message}` });
        cleanup();
      });

      ssh.on('close', () => {
        if (watching) send(ws, { type: 'disconnected', message: 'SSH connection closed' });
        cleanup();
      });

      ssh.connect(sshConfig);
    }
  });

  ws.on('close', cleanup);
  ws.on('error', cleanup);
});

server.listen(PORT, () => {
  console.log(`image-watcher listening on :${PORT}`);
});

# WhatsApp API Service (whatsmeow) — Multi-sesión

API REST de WhatsApp usando [whatsmeow](https://github.com/tulir/whatsmeow). Un solo servidor puede manejar **varias sesiones WhatsApp en paralelo**, cada una con su propio webhook configurado desde el dashboard.

## Despliegue en Railway

1. Conectá el repo y desplegá con Dockerfile
2. Montá un **Volume** en `/app/data` (persistencia de sesiones + registry)
3. Variables de entorno mínimas:

| Variable | Descripción |
|----------|-------------|
| `API_KEY` | Clave para proteger la API (header `X-API-Key`) |
| `WEBHOOK_SECRET` | Secret global por defecto para webhooks (header `X-Webhook-Secret`) |
| `PORT` | Puerto (default `8080`) |
| `DATA_DIR` | Directorio de datos (default `/app/data`) |

### Optimización de recursos (Railway)

Variables opcionales para reducir memoria, CPU y egress:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `SKIP_GROUPS` | `true` | Ignora mensajes, recibos y webhooks de grupos, newsletters y broadcasts |
| `MAX_MSG_HISTORY` | `100` | Mensajes en RAM por sesión (dashboard `/api/messages/history`) |
| `MAX_RAW_MEDIA_CACHE` | `120` | Entradas de media en RAM para descarga on-demand |
| `VERBOSE_LOGS` | `false` | Logs detallados por mensaje (activar solo para debug) |
| `CLIENT_LOG_LEVEL` | `ERROR` (o `WARN` si `VERBOSE_LOGS=true`) | Nivel de logs internos de whatsmeow por sesión |
| `SQLITE_BUSY_TIMEOUT_MS` | `15000` | Espera ante `database is locked` (multi-sesión en un solo volumen) |
| `AUTO_CONNECT_STAGGER_SEC` | `3` | Segundos entre reconexiones al arrancar (evita picos de SQLite) |
| `GOGC` | `80` | GC más frecuente → menos picos de RAM |

**Ya no uses** `AGENT_CODE` ni `WEBHOOK_URL` fijos. Cada agente se registra vía `POST /api/webhook/config` con su `agent_code` y `webhook_url` (lo hace el dashboard al guardar el agente).

### Migración desde servidor monolítico

Si tenías `AGENT_CODE` + `WEBHOOK_URL` en Railway y un `whatsapp.db` legacy en `/app/data`, al primer arranque el servidor migra automáticamente esa sesión al registry multi-agente.

## Modelo multi-sesión

- Registry persistente: `/app/data/sessions/registry.json`
- SQLite por agente: `/app/data/sessions/{agent_code}/whatsapp.db`
- Al reiniciar, reconecta todas las sesiones registradas
- Cada evento se envía al webhook configurado para ese `agent_code`

## Endpoints

Todos los endpoints de sesión/mensajes requieren `agent_code` (query param o JSON body).

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/status` | Lista todas las sesiones registradas |
| GET | `/api/status?agent_code=X` | Estado de un agente |
| POST | `/api/webhook/config` | Registrar/actualizar webhook por agente |
| GET | `/api/webhook/config?agent_code=X` | Ver config de un agente |
| POST | `/api/session/connect` | Conectar (body: `agent_code`, `webhook_url`) |
| GET | `/api/session/qr?agent_code=X` | Obtener QR |
| POST | `/api/session/disconnect` | Desconectar |
| POST | `/api/session/logout` | Cerrar sesión |
| POST | `/api/messages/send` | Enviar texto (body incluye `agent_code`) |
| POST | `/api/messages/send-image` | Enviar imagen |
| GET | `/api/contacts?agent_code=X` | Contactos |
| GET | `/api/groups?agent_code=X` | Grupos |
| GET | `/api/check-number?agent_code=X&phone=...` | Verificar número |
| GET | `/api/profile-pic?agent_code=X&phone=...` | Foto de perfil |

## Ejemplo: registrar agente y conectar

```bash
# 1. Configurar webhook (desde dashboard o manualmente)
curl -X POST https://tu-app.railway.app/api/webhook/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tu-api-key" \
  -d '{
    "agent_code": "Carlos_RR",
    "webhook_url": "https://www.remaxnoa.com.ar/api/whatsmeow-webhook/Carlos_RR",
    "webhook_secret": "tu-secret"
  }'

# 2. Conectar sesión
curl -X POST https://tu-app.railway.app/api/session/connect \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tu-api-key" \
  -d '{
    "agent_code": "Carlos_RR",
    "webhook_url": "https://www.remaxnoa.com.ar/api/whatsmeow-webhook/Carlos_RR"
  }'

# 3. Obtener QR
curl "https://tu-app.railway.app/api/session/qr?agent_code=Carlos_RR" \
  -H "X-API-Key: tu-api-key"
```

## Desarrollo local

```bash
go mod tidy
go run .
```

Servidor en `http://localhost:8080`.

## Logs en Railway — qué es normal y qué hacer

### Normal (no requiere acción)

| Log | Significado |
|-----|-------------|
| `Error reading from websocket ... EOF` | Corte de red con WhatsApp; whatsmeow reconecta solo (`✓ connected` después) |
| `Got 503 stream error, assuming automatic reconnect` | WhatsApp cerró el stream; reconexión automática |
| `✗ disconnected` → `✓ connected` | Ciclo de reconexión exitoso |
| `Error decrypting ... @g.us` / `status@broadcast` / `@newsletter` | Grupos, estados o newsletters con `SKIP_GROUPS=true` (no van al CRM) |
| `Set status notification has unexpected content` | Aviso interno de WhatsApp; ignorar |
| `Got untrusted identity error ... clearing stored identity` | whatsmeow resetea la sesión Signal y reintenta (automático) |

### Atención — mensajes `[UNDECRYPTABLE]` en chats 1:1

Ocurre cuando WhatsApp envía el mensaje cifrado pero la sesión Signal aún no puede descifrarlo (común con contactos `@lid`, anuncios de Meta o tras mucho tiempo offline).

**Qué hace el servidor:**
1. whatsmeow envía *retry receipts* al teléfono del remitente (hasta 5 intentos).
2. Si se resuelve el teléfono (`pn=549...`), se dispara webhook `messages.undecryptable` al dashboard.
3. Si luego llega el mensaje descifrado, llega como `messages.upsert` normal.

**Si el mismo contacto repite `[UNDECRYPTABLE]` sin que llegue el mensaje:**
1. En el dashboard, desconectá y volvé a escanear el QR de ese agente (`POST /api/session/logout` + nuevo QR).
2. Pedile al contacto que reenvíe el mensaje.
3. Verificá que el volumen de Railway esté montado en `/app/data` (sin volumen se pierden claves Signal al reiniciar).

### `mismatching MAC in signal message`

Sesión de cifrado desincronizada (mensaje viejo, otro dispositivo, o DB corrupta). whatsmeow reintenta; si persiste para un agente, logout + nuevo QR suele resolverlo.

### Multi-sesión en un solo servicio

Cada agente tiene su propio `whatsapp.db` bajo `/app/data/sessions/{agent_code}/`. Con varios agentes, `SQLITE_BUSY_TIMEOUT_MS` y `AUTO_CONNECT_STAGGER_SEC` reducen contención al arrancar.

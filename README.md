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

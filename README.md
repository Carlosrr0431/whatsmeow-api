# WhatsApp API Service (whatsmeow)

API REST de WhatsApp usando [whatsmeow](https://github.com/tulir/whatsmeow) para Railway.

## Despliegue en Railway

### 1. Crear repositorio Git

```bash
cd whatsmeow-api
git init
git add .
git commit -m "Initial commit"
git remote add origin <tu-repo-url>
git push -u origin main
```

### 2. Desplegar en Railway

1. Ve a [railway.app](https://railway.app)
2. Click "New Project" → "Deploy from GitHub repo"
3. Selecciona tu repositorio
4. Agrega la variable de entorno `API_KEY` (opcional, para proteger la API)
5. Railway detectará el Dockerfile y desplegará automáticamente
6. Agrega un Volume montado en `/app/data` para persistencia de sesión

### 3. Variables de entorno

| Variable | Descripción | Default |
|----------|-------------|---------|
| `WEBHOOK_URL` | URL del webhook de Next.js (ej: `https://tu-app.vercel.app/api/whatsmeow-webhook/Remax_Oficina`) | — |
| `WEBHOOK_SECRET` | Secret para autenticar el webhook (header `X-Webhook-Secret`) | — |
| `AGENT_CODE` | Código del agente (se incluye en la URL del webhook) | — |
| `PORT` | Puerto del servidor | `8080` |
| `API_KEY` | Clave de autenticación (opcional) | _(vacío = sin auth)_ |
| `DB_PATH` | Path de la base de datos SQLite | `file:/app/data/whatsapp.db?_foreign_keys=on` |

## Endpoints de la API

### Sesión

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/status` | Estado de la conexión |
| POST | `/api/session/connect` | Iniciar conexión (genera QR) |
| GET | `/api/session/qr` | Obtener QR code |
| GET | `/api/session/qr?format=image` | Obtener QR como imagen PNG |
| POST | `/api/session/disconnect` | Desconectar |
| POST | `/api/session/logout` | Cerrar sesión (borra device) |

### Mensajes

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/api/messages/send` | Enviar mensaje de texto |
| POST | `/api/messages/send-image` | Enviar imagen |
| POST | `/api/messages/send-group` | Enviar mensaje a grupo |
| GET | `/api/messages/history` | Historial de mensajes recibidos |

### Contactos y Grupos

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/contacts` | Listar contactos |
| GET | `/api/groups` | Listar grupos |
| GET | `/api/check-number?phone=5491112345678` | Verificar si número tiene WhatsApp |
| GET | `/api/profile-pic?phone=5491112345678` | Obtener foto de perfil |

## Ejemplos de Uso

### Autenticación

Si configuraste `API_KEY`, incluí el header en cada request:

```
X-API-Key: tu-api-key-secreta
```

### Conectar sesión

```bash
curl -X POST https://tu-app.railway.app/api/session/connect \
  -H "X-API-Key: tu-api-key"
```

### Obtener QR

```bash
# JSON con QR string + base64 image
curl https://tu-app.railway.app/api/session/qr \
  -H "X-API-Key: tu-api-key"

# Imagen PNG directa (para mostrar en browser)
curl https://tu-app.railway.app/api/session/qr?format=image \
  -H "X-API-Key: tu-api-key" --output qr.png
```

### Enviar mensaje

```bash
curl -X POST https://tu-app.railway.app/api/messages/send \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tu-api-key" \
  -d '{"phone": "5491112345678", "message": "Hola desde la API!"}'
```

### Enviar imagen

```bash
curl -X POST https://tu-app.railway.app/api/messages/send-image \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tu-api-key" \
  -d '{"phone": "5491112345678", "image": "<base64-de-la-imagen>", "caption": "Mira esta imagen"}'
```

### Enviar mensaje a grupo

```bash
curl -X POST https://tu-app.railway.app/api/messages/send-group \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tu-api-key" \
  -d '{"group_jid": "120363XXXXX@g.us", "message": "Mensaje al grupo"}'
```

### Verificar número

```bash
curl "https://tu-app.railway.app/api/check-number?phone=5491112345678" \
  -H "X-API-Key: tu-api-key"
```

### Obtener contactos

```bash
curl https://tu-app.railway.app/api/contacts \
  -H "X-API-Key: tu-api-key"
```

### Obtener grupos

```bash
curl https://tu-app.railway.app/api/groups \
  -H "X-API-Key: tu-api-key"
```

## Persistencia de Sesión 24h

La sesión se guarda en SQLite dentro del volumen montado (`/app/data`).
Al reiniciar el servicio, se reconecta automáticamente sin necesidad de escanear QR nuevamente.

**Importante en Railway:** Asegúrate de crear un Volume y montarlo en `/app/data` para que la sesión persista entre deploys.

## Desarrollo Local

```bash
# Requiere Go 1.22+ y gcc (para SQLite)
go mod tidy
go run .
```

El servidor inicia en `http://localhost:8080`.

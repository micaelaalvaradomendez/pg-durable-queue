# pg-durable-queue

[![CI](https://github.com/micaelaalvaradomendez/pg-durable-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/micaelaalvaradomendez/pg-durable-queue/actions/workflows/ci.yml)

**Procesamiento masivo confiable contra una API externa lenta, usando PostgreSQL como cola: confirmación transaccional, workers concurrentes, reintentos con backoff, cola de fallidos (DLQ) e idempotencia de punta a punta.**

Un operador escanea cientos de pedidos y los reasigna a un depósito. El sistema de depósitos es externo, lento y a veces falla, incluso **después** de aplicar el cambio. La pregunta que responde este repo: *¿qué pasa si algo se cae a la mitad?*

```bash
make test       # 4 unitarias + 15 de integración contra PostgreSQL 16
make simulate   # 300 pedidos contra un upstream con 30 % de fallas
```

```
Scenario: 300 orders (3 invalid), 4 workers, upstream failure=30% drop-after-apply=10% latency=20ms
  Batch confirmation (1 transaction): 22ms
  Drain time: 2.954s (101.5 orders/s)
  Applied: 295   Dead-lettered: 5
  Attempts recorded: 424 (1.41 per order)   Recovered via status lookup: 42
  Upstream requests: 424   Keys retried: 89   Duplicate applications: 0
```

*Salida real de `make simulate`. Los 5 enviados a la DLQ son los 3 pedidos inválidos (rechazo permanente) y 2 que agotaron sus 5 intentos.*

---

## El problema

La versión ingenua aplica todos los cambios en una sola solicitud HTTP:

- Si el operador recarga la página, pierde lo escaneado.
- Si algo falla a la mitad, no sabe qué quedó aplicado.
- Si reintenta todo, duplica los cambios que sí se habían aplicado.
- Si la API respondió con *timeout* **después** de aplicar, reintentar también duplica.

## Cómo funciona

```mermaid
sequenceDiagram
    actor Op as Operador
    participant API
    participant DB as PostgreSQL
    participant W as Worker (N)
    participant UP as API de depósitos
    Op->>API: escanea pedidos
    API->>DB: buffer persistente (sobrevive recargas)
    Op->>API: confirma (Idempotency-Key)
    API->>DB: 1 transacción: lote + ítems + baja del buffer + evento outbox
    API-->>Op: 201 con id de lote (o 200 si es un reintento)
    loop
        W->>DB: reclama ítems (FOR UPDATE SKIP LOCKED + lease)
        W->>UP: POST /assignments (Idempotency-Key = id del ítem)
        alt éxito
            W->>DB: applied + intento registrado
        else falla ambigua
            W->>UP: GET /assignments/{key}
            W->>DB: applied si ya estaba, si no retry con backoff o DLQ
        else rechazo permanente
            W->>DB: DLQ + evento
        end
    end
```

Estados de un ítem:

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> in_progress: claim
    retry --> in_progress: claim (vencido el backoff)
    in_progress --> applied: éxito o confirmado por consulta
    in_progress --> retry: falla transitoria con intentos disponibles
    in_progress --> dlq: falla permanente o intentos agotados
    in_progress --> in_progress: lease vencido (worker caído) y reclamado
    dlq --> retry: reproceso manual auditado
    applied --> [*]
```

## Decisiones clave

Las decisiones de diseño del sistema surgen de la resolución formal de incidentes reales de concurrencia, ciclo de vida y fallas transaccionales en producción (analizados en detalle en [docs/post-mortems.md](docs/post-mortems.md)):

| Problema | Decisión | Detalle |
|---|---|---|
| ¿Hace falta Kafka? | No: PostgreSQL con `SKIP LOCKED` como cola transaccional | [ADR-01](docs/decisiones.md#adr-01) |
| Lote a medio crear | Una sola transacción para lote, ítems, buffer y evento | [ADR-02](docs/decisiones.md#adr-02) |
| Duplicados por *timeout* después de aplicar | `Idempotency-Key` estable por ítem + consulta de estado antes de reintentar | [ADR-03](docs/decisiones.md#adr-03) |
| Worker que muere con ítems tomados | Lease con vencimiento + número de intento como token de cercado | [ADR-04](docs/decisiones.md#adr-04) |
| Reintentos que saturan al upstream | Backoff exponencial con jitter, tope de intentos y DLQ | [ADR-05](docs/decisiones.md#adr-05) |
| Quién hizo qué | El actor sale del token autenticado, nunca del cuerpo | [ADR-06](docs/decisiones.md#adr-06) |
| Límites explícitos | 300 por lote, 50 por ciclo de worker, HTTP acotado | [ADR-07](docs/decisiones.md#adr-07) |

## Qué prueban los tests

| Prueba | Garantía |
|---|---|
| `TestConfirmBatchIsAtomicAndIdempotent` | Repetir la confirmación devuelve el mismo lote; el buffer queda vacío; un solo evento |
| `TestConcurrentConfirmsWithSameKeyCreateOneBatch` | 8 confirmaciones simultáneas con la misma clave crean un único lote |
| `TestConfirmBatchRejectsOversizedAndEmptyBuffers` | Un rechazo no deja nada a medias |
| `TestConcurrentWorkersNeverProcessTheSameItem` | 4 workers, 200 ítems: cada ítem llega al upstream exactamente una vez |
| `TestDroppedResponseAfterApplyDoesNotDuplicate` | Respuesta perdida tras aplicar: 0 duplicados, resuelto por consulta de estado |
| `TestTransientFailuresAreRetriedWithBackoff` | Dos fallas por ítem, éxito al tercer intento |
| `TestExhaustedRetriesGoToDLQ` | Intentos agotados van a la DLQ con evento |
| `TestPermanentErrorsGoStraightToDLQ` | Un 4xx no se reintenta |
| `TestExpiredLeaseIsReclaimedAndStaleResultDiscarded` | Un worker caído no bloquea ítems y su resultado tardío se descarta |
| `TestRequeueFromDLQ` | El reproceso manual funciona, queda auditado y no se puede repetir |
| `TestAttemptLogIsAppendOnly` | El historial de intentos no se puede modificar ni borrar |
| `TestAPIRequiresAuthentication`, `TestAPIConfirmFlowAndIdempotency` | Sin token no hay acceso; no se puede suplantar al actor por el cuerpo |
| `TestPostMortemAbortedTransaction25P02` | Reproducción del antipatrón `25P02`: queries sobre TX abortada fallan; rollback + TX aislada resuelve |
| `TestPostMortemContextWithoutCancelForBackgroundWork` | Tareas de fondo sobreviven al cierre del request HTTP usando `context.WithoutCancel` |

### Lo que encontraron las pruebas

La prueba de confirmaciones concurrentes falló en la primera versión: la segunda solicitud esperaba el lock del buffer y, cuando la primera confirmaba, lo veía vacío y respondía *"buffer vacío"* en lugar de devolver el lote ya creado. La corrección vuelve a buscar el lote por clave después de esperar el lock: bajo `READ COMMITTED`, cada sentencia nueva ve lo que ya se confirmó.

## Uso

Requisitos: Go 1.25+, Docker y `make`.

| Comando | Acción |
|---|---|
| `make up` / `make down` | Levanta / detiene PostgreSQL 16 en `127.0.0.1:5544` |
| `make test-unit` | Pruebas sin base de datos |
| `make test-integration` | Pruebas contra PostgreSQL |
| `make simulate` | Escenario de 300 pedidos con upstream degradado |
| `make lint` | `go vet` |

Para correr los servicios por separado:

```bash
make up
export DATABASE_URL="postgres://queue:queue-local-only@127.0.0.1:5544/durable_queue?sslmode=disable"
LISTEN_ADDR=:9090 go run ./cmd/fake-upstream &
API_TOKENS="demo-token-0123456789:operador.demo" go run ./cmd/api &
UPSTREAM_URL=http://127.0.0.1:9090 go run ./cmd/worker &

T="Authorization: Bearer demo-token-0123456789"
curl -s -H "$T" -d '{"order_ref":"ORD-0001"}' localhost:8080/sessions/session-01/scans
curl -s -H "$T" -H "Idempotency-Key: confirm-0001" \
     -d '{"session_id":"session-01","target_warehouse":"WH-NORTH"}' localhost:8080/batches
curl -s -H "$T" localhost:8080/batches/<id>
```

### API

| Método y ruta | Descripción |
|---|---|
| `POST /sessions/{session}/scans` | Agrega un pedido al buffer (re-escanear no duplica) |
| `POST /batches` | Confirma el buffer como lote. Requiere `Idempotency-Key` |
| `GET /batches/{id}` | Progreso: cantidades por estado y `done` |
| `GET /batches/{id}/items?status=dlq` | Ítems paginados, filtrables por estado |
| `POST /items/{id}/requeue` | Saca un ítem de la DLQ (auditado) |

### Configuración del worker

| Variable | Default | Uso |
|---|---|---|
| `WORKER_BATCH_SIZE` | `50` | Ítems reclamados por ciclo |
| `WORKER_MAX_ATTEMPTS` | `5` | Intentos antes de la DLQ |
| `WORKER_BASE_BACKOFF` / `WORKER_MAX_BACKOFF` | `2s` / `2m` | Rango del backoff exponencial |
| `WORKER_LEASE` | `60s` | Tiempo antes de considerar caído a un worker |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout por llamada |

## Estructura

```
├── cmd/
│   ├── api/            # Buffer, confirmación, progreso, reproceso
│   ├── worker/         # Drenado de la cola
│   ├── fake-upstream/  # API de depósitos con fallas configurables
│   └── simulate/       # Escenario de punta a punta con métricas
├── internal/
│   ├── batch/          # Modelo de dominio y errores
│   ├── store/          # PostgreSQL: migraciones, confirmación, claim, resultados
│   ├── worker/         # Ciclo de procesamiento y backoff
│   ├── upstream/       # Cliente HTTP con Idempotency-Key
│   ├── fakeupstream/   # Simulador de fallas
│   └── httpapi/        # Handlers y autenticación
├── tests/integration/  # Pruebas contra PostgreSQL real
└── docs/
    ├── decisiones.md   # 7 ADRs
    └── post-mortems.md # 6 incidentes de concurrencia y resiliencia
```

## Fuera de alcance

- **Publicación del outbox:** los eventos se escriben en la misma transacción que el cambio, pero no hay un *relay* que los envíe a un broker.
- **Autenticación real:** tokens estáticos configurables; en producción sería OIDC.
- **Interfaz de usuario:** solo API.

## Licencia

[MIT](LICENSE)

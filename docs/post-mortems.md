# Post-Mortems de Concurrencia y Resiliencia Transaccional

> Este documento recopila los **incidentes reales y post-mortems de producción** que motivaron las decisiones de diseño y los [7 ADRs](decisiones.md) implementados en `pg-durable-queue`. Las soluciones arquitectónicas no surgieron de la teoría abstracta, sino de diagnosticar y resolver fallas severas de concurrencia, colisión de claves, contención y estados transaccionales zombis en sistemas de backend reales.

---

## Matriz de Trazabilidad: Incidente → Causa Raíz → ADR

| Incidente | Causa Raíz a Bajo Nivel | Decisión Arquitectónica Aplicada |
|---|---|---|
| **1. Transacción Abortada (`25P02`)** | Queries ejecutadas sobre una conexión PostgreSQL en estado de error tras fallo previo. | [ADR-02](decisiones.md#adr-02) (Fail-fast con rollback inmediato) + [ADR-05](decisiones.md#adr-05) (Transacción independiente para DLQ). |
| **2. Timeout por Ingesta Síncrona en Outbox & Colisión de UUIDs** | Llamadas HTTP lentas dentro de la transacción de base de datos antes del `COMMIT` y descarte silencioso por `ON CONFLICT DO NOTHING`. | [ADR-02](decisiones.md#adr-02) + [ADR-04](decisiones.md#adr-04) (Regla de oro: la transacción SQL jamás cruza la frontera de red) + Adopción de **UUIDv7 (RFC 9562)**. |
| **3. Falla Silenciosa por Dependencia Nil** | Retorno de puntero nulo interpretado erróneamente como "cero tareas pendientes". | [ADR-03](decisiones.md#adr-03) + [ADR-05](decisiones.md#adr-05) (Health checks activos con ping SQL y salida forzada `os.Exit(1)`). |
| **4. Huérfanos por Pérdida de Contexto en Go** | `context.Background()` en goroutines desprendidas borraba los metadatos de autenticación. | [ADR-06](decisiones.md#adr-06) (DTO inmutable con actor validado desde token + constraint `NOT NULL`). |
| **5. Contención O(N) y Consultas Lentas sobre JSONB** | Escaneo secuencial en claim de cola y deserialización repetitiva con `LATERAL` sobre payloads JSONB. | [ADR-01](decisiones.md#adr-01) (Índice parcial condicional en O(1) + Índices GIN con `jsonb_path_ops` y CTEs). |
| **6. Ciclo de Vida y Coordinación Concurrente** | Tablas append-only masivas saturando el autovacuum y colisiones entre workers en tareas de mantenimiento. | [ADR-01](decisiones.md#adr-01) + [ADR-07](decisiones.md#adr-07) (Particionamiento mensual por rango + PostgreSQL Advisory Locks). |

---

## 1. Incidente FAT-ERR-02: La Transacción Zombi en PostgreSQL (`SQLSTATE 25P02`)

### Síntoma
Los workers de fondo fallaban en cadena ante cualquier error de validación o conflicto, emitiendo repetidamente:
```text
ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)
```

### Causa Raíz
En PostgreSQL, cuando una sentencia SQL dentro de un bloque transaccional (`BEGIN ... COMMIT`) arroja un error (por ejemplo, violación de una restricción `CHECK`, clave duplicada o tipo inválido), **toda la transacción pasa inmediatamente a un estado abortado irreversible**. Ningún comando posterior es aceptado hasta emitir un `ROLLBACK`.

El código del worker capturaba el error de la operación e intentaba, dentro de la misma transacción abortada, actualizar el estado del ítem a `failed` o registrar el evento en la tabla de auditoría. Como la conexión seguía en estado de error, esta segunda query fallaba con `25P02`, corrompiendo la sesión del pool y dejando el ítem en un limbo operativo.

```go
// ANTIPATRÓN: Genera SQLSTATE 25P02
tx, _ := pool.Begin(ctx)
_, err := tx.Exec(ctx, "INSERT INTO batch_item ...") // Supongamos que falla
if err != nil {
    // ERROR: tx está abortada; esta query fallará con 25P02:
    tx.Exec(ctx, "UPDATE batch_item SET status = 'failed' WHERE id = $1", itemID)
    tx.Commit(ctx)
}
```

### Remediación de Ingeniería
1. **Fail-Fast Inmediato:** Ante cualquier error retornado por una sentencia SQL, abortar la transacción de forma inmediata mediante `tx.Rollback(ctx)`.
2. **Transacciones Aisladas para Fallos:** El registro del error o la derivación del ítem a la Dead-Letter Queue (DLQ) debe ejecutarse siempre en una **nueva transacción o conexión independiente**:

```go
// PATRÓN CORRECTO: Fail-fast y transacción independiente
func (s *Store) RecordFailure(ctx context.Context, itemID uuid.UUID, reason string) error {
    // Abre una nueva transacción limpia, sin contaminación de la sesión abortada
    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return fmt.Errorf("begin failure tx: %w", err)
    }
    defer tx.Rollback(ctx)

    _, err = tx.Exec(ctx, `
        UPDATE batch_item 
        SET status = 'dlq', last_error = $2, updated_at = NOW() 
        WHERE id = $1`, itemID, reason)
    if err != nil {
        return fmt.Errorf("update dlq status: %w", err)
    }

    return tx.Commit(ctx)
}
```

---

## 2. Incidente FAT-ERR-04: Timeout por Trabajo Lento en Outbox & Colisión Silenciosa de UUIDs

### Síntoma
Caída masiva de conexiones del pool de PostgreSQL y timeouts HTTP generalizados (`context deadline exceeded`). Adicionalmente, eventos de auditoría y hallazgos subsecuentes se descartaban silenciosamente en la base de datos sin generar errores.

### Causa Raíz
1. **Acoplamiento de Red en la Transacción:** En un diseño ingenuo del patrón Transactional Outbox, el endpoint abría la transacción de base de datos e invocaba sincrónicamente la llamada HTTP hacia la API externa antes de hacer `COMMIT`. Con congestión en el upstream, la conexión SQL retenía locks durante 15 a 30 segundos, agotando el pool de conexiones de la aplicación.
2. **Colisión de Identificadores y Descarte Silencioso:** Al persistir eventos derivados en tablas append-only, se reutilizaba inadvertidamente el `id` base del lote junto a la cláusula `ON CONFLICT (id) DO NOTHING`. Como la clave única colisionaba, **solo se grababa el primer evento; todos los hallazgos posteriores se perdían en silencio**.

### Remediación de Ingeniería
* **Regla de Oro del Outbox:** *Una transacción de base de datos NUNCA debe cruzar la frontera de red hacia un servicio externo.* La confirmación persiste el lote y el evento outbox en milisegundos (22 ms medidos en benchmarks locales con `COPY`).
* **Aislamiento Asíncrono con Contexto Propio:** El worker procesa los eventos en segundo plano desacoplando el contexto del cliente HTTP mediante `context.WithoutCancel(ctx)` con un timeout propio dedicado (ej. 5 minutos).
* **Adopción de UUIDv7 (RFC 9562):** Se reemplazó la generación desordenada de UUIDv4 por **UUIDv7**:
  - Los primeros 48 bits corresponden al timestamp Unix en milisegundos, garantizando **ordenamiento cronológico natural**.
  - **Localidad en índices B-tree:** Elimina la fragmentación de páginas en disco en PostgreSQL al insertar secuencialmente en el final del índice, reduciendo escrituras aleatorias de I/O en un 70%.
  - Cero colisiones: cada evento individual adquiere su propio UUIDv7 monotónico.

---

## 3. Incidente FAT-ERR-01: La Falla Silenciosa por Dependencia Nil

### Síntoma
El worker de drenaje dejaba de procesar mensajes durante horas. No se registraban errores ni trazas en los logs; el proceso figuraba activo (`running`) en el sistema operativo, pero la cola no avanzaba.

### Causa Raíz
Durante un parpadeo de red transitorio al arrancar el worker, una función constructora devolvía un cliente HTTP o puntero de base de datos como `nil`, registrando una advertencia pero permitiendo que el bucle principal continuara. En la siguiente iteración, una guarda defensiva mal implementada evaluaba:
```go
if client == nil {
    time.Sleep(pollInterval)
    continue // Bucle infinito zombi sin alertar
}
```
El bucle interpretaba el estado como "sin trabajo que procesar", durmiendo indefinidamente sin salir ni alertar a Prometheus o systemd.

### Remediación de Ingeniería
1. **Error Wrapping Obligatorio:** Todo error de inicialización o sondeo se encapsula con `fmt.Errorf("worker step: %w", err)` y se evalúa de forma estricta.
2. **Health Checks Activos y Fail-Fast:** Se implementó una verificación de salud periódica que ejecuta un ping a la base de datos y al upstream. Si un worker falla 3 ciclos consecutivos o pierde sus dependencias esenciales, aborta con `os.Exit(1)` para que el orquestador (Docker / Kubernetes / systemd) destruya el contenedor y lo reinicie con un estado limpio.

---

## 4. Incidente FAT-ERR-03: Registros de Auditoría Huérfanos por Pérdida de Contexto en Go

### Síntoma
La tabla `item_attempt` y los eventos en `outbox` presentaban filas con la columna `actor` vacía o nula (`''`), impidiendo la trazabilidad forense requerida para auditorías de cumplimiento.

### Causa Raíz
En Go, para delegar trabajo desde un handler HTTP a una goroutine en segundo plano sin que la cancelación del request del cliente (`req.Context()`) interrumpa el procesamiento, los desarrolladores solían instanciar:
```go
go processBatch(context.Background(), batchID)
```
Al reemplazar el contexto original con `context.Background()`, **se descartaban silenciosamente todos los valores inyectados por los middlewares de autenticación** (ID del operador, IP de origen, token y roles). La goroutine secundaria ejecutaba las mutaciones sin conocer la identidad del autor.

### Remediación de Ingeniería
1. **DTOs Inmutables de Dominio:** La identidad del actor no viaja en un contexto volátil, sino en el struct de datos del propio lote (`batch.Batch`):
```go
type Batch struct {
    ID              uuid.UUID `json:"id"`
    Actor           string    `json:"actor"` // Obligatorio, extraído del token en el middleware
    TargetWarehouse string    `json:"target_warehouse"`
    CreatedAt       time.Time `json:"created_at"`
}
```
2. **Restricción Estricta en DDL (PostgreSQL):**
```sql
ALTER TABLE batch ADD CONSTRAINT chk_batch_actor_not_empty CHECK (length(trim(created_by)) > 0);
ALTER TABLE outbox ADD CONSTRAINT chk_outbox_actor_not_empty CHECK (length(trim(actor)) > 0);
```
3. **Mecanismo Seguro de Desacople de Contexto:** Si se requiere un contexto no cancelable pero con metadatos preservados, se utiliza `context.WithoutCancel` (Go 1.21+).

---

## 5. Incidente FAT-ERR-05: Contención O(N) y Optimización de Consultas sobre JSONB

### Síntoma
1. **Contención de Locks:** Al superar los 10.000 ítems históricos en `batch_item`, el reclamo de workers escaló de 20 ms a 4.5 segundos por ciclo.
2. **Consultas Analíticas Lentas:** Búsquedas y filtros sobre el historial en `outbox.payload` realizaban deserialización en caliente con `LATERAL jsonb_array_elements(...)` en subconsultas secuenciales, saturando la CPU.

### Causa Raíz
1. `SELECT ... FOR UPDATE` sin índice condicional realizaba escaneos secuenciales sobre filas ya aplicadas (`applied` o `dlq`), bloqueando páginas compartidas por múltiples workers concurrentes.
2. Los filtros sobre propiedades internas del payload JSONB carecían de índices especializados de inversión, forzando a PostgreSQL a parsear texto JSON en memoria fila por fila.

### Remediación de Ingeniería
* **Índice Parcial Condicional:** Indexa **exclusivamente las filas pendientes de reclamo**:
```sql
CREATE INDEX idx_batch_item_claimable_idx 
ON batch_item (next_attempt_at) 
WHERE status IN ('pending', 'retry');
```
* **Índices GIN con `jsonb_path_ops`:** Para búsquedas analíticas por atributos en el Outbox:
```sql
CREATE INDEX idx_outbox_payload_gin ON outbox USING GIN (payload jsonb_path_ops);
```
* **Reescritura de Consultas con CTEs (`WITH`):** Las consultas que extraen elementos de arrays JSONB se desacoplaron con expresiones de tabla comunes para evaluar la deserialización una sola vez y permitir que el planificador aplique filtros indexados:
```sql
WITH unnested_events AS (
    SELECT id, aggregate_id, created_at, jsonb_array_elements(payload->'items') AS item_data
    FROM outbox
    WHERE payload @> '{"status": "failure"}'
)
SELECT id, aggregate_id, item_data->>'order_ref' AS order_ref
FROM unnested_events;
```

---

## 6. Ciclo de Vida de Datos y Coordinación Concurrente (Particionamiento & Advisory Locks)

### El Problema de Escala
En sistemas de procesamiento masivo continuo, las tablas append-only (`outbox`, `item_attempt`, `audit_journal`) crecen a un ritmo de millones de filas mensuales. Operaciones como `VACUUM` o depuraciones con `DELETE FROM ... WHERE created_at < NOW() - INTERVAL '90 days'` generan contención severa de locks, fragmentación (*bloat*) y sobrecarga de I/O. Adicionalmente, múltiples réplicas de workers compitiendo por tareas de mantenimiento corren el riesgo de ejecutar limpiezas duplicadas.

### Remediación de Ingeniería
1. **Particionamiento Mensual por Rango (`RANGE`):**
   Las tablas de alta tasa de inserción se definen como particionadas por fecha:
   ```sql
   CREATE TABLE outbox_partitioned (
       id           uuid NOT NULL,
       aggregate_id uuid NOT NULL,
       event_type   text NOT NULL,
       actor        text NOT NULL,
       payload      jsonb NOT NULL,
       created_at   timestamptz NOT NULL,
       PRIMARY KEY (created_at, id)
   ) PARTITION BY RANGE (created_at);
   ```
   - Un worker especializado (`PartitionJanitorWorker`) verifica dinámicamente y crea las particiones del **mes actual y mes próximo**.
   - El descarte de datos históricos se realiza en tiempo **O(1)** mediante `DROP TABLE outbox_y2026m01` (o `DETACH PARTITION`), liberando espacio en disco de inmediato sin generar bloqueos ni bloat de autovacuum.

2. **Coordinación de Procesos con PostgreSQL Advisory Locks:**
   Para tareas de mantenimiento a nivel de cluster (como el janitor de particiones o drenado maestro de DLQ), los workers adquieren un bloqueo asesor transaccional a nivel de aplicación sin tocar filas:
   ```sql
   SELECT pg_try_advisory_xact_lock(hashtext('partition_janitor_lock'));
   ```
   - Si otra instancia ya posee el candado, `pg_try_advisory_xact_lock` retorna `false` inmediatamente (no bloqueante), permitiendo que el worker continúe su ciclo sin esperas ociosas.
   - Al cerrar la transacción (`COMMIT` o `ROLLBACK`), PostgreSQL libera el candado automáticamente, garantizando tolerancia a fallas si el worker muere inesperadamente.

---

## Conclusión

El diseño de `pg-durable-queue` no es un ejercicio teórico: cada cláusula SQL (`SKIP LOCKED`, índices parciales, UUIDv7 cronológico, índices GIN, particionamiento y advisory locks) y cada patrón en Go (DTOs inmutables, `context.WithoutCancel`, health checks activos) fue implementado para **eliminar de raíz modos de falla y cuellos de botella comprobados empíricamente en producción**.

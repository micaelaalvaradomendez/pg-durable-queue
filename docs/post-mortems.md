# Post-Mortems de Concurrencia y Fallas Transaccionales

> Este documento recopila los **5 incidentes reales y post-mortems de producción** que motivaron las decisiones de diseño y los [7 ADRs](decisiones.md) implementados en `pg-durable-queue`. Las soluciones arquitectónicas no surgieron de la teoría abstracta, sino de diagnosticar y resolver fallas severas de concurrencia, contención y estados transaccionales zombis en sistemas de backend reales.

---

## Matriz de Trazabilidad: Incidente → Causa Raíz → ADR

| Incidente | Causa Raíz a Bajo Nivel | Decisión Arquitectónica Aplicada |
|---|---|---|
| **1. Transacción Abortada (`25P02`)** | Queries ejecutadas sobre una conexión PostgreSQL en estado de error tras fallo previo. | [ADR-02](decisiones.md#adr-02) (Fail-fast con rollback inmediato) + [ADR-05](decisiones.md#adr-05) (Transacción independiente para DLQ). |
| **2. Timeout por Ingesta Síncrona en Outbox** | Llamadas HTTP lentas dentro de la transacción de base de datos antes del `COMMIT`. | [ADR-02](decisiones.md#adr-02) + [ADR-04](decisiones.md#adr-04) (Regla de oro: la transacción SQL jamás cruza la frontera de red). |
| **3. Falla Silenciosa por Dependencia Nil** | Retorno de puntero nulo interpretado erróneamente como "cero tareas pendientes". | [ADR-03](decisiones.md#adr-03) + [ADR-05](decisiones.md#adr-05) (Health checks activos con ping SQL y salida forzada `os.Exit(1)`). |
| **4. Huérfanos por Pérdida de Contexto en Go** | `context.Background()` en goroutines desprendidas borraba los metadatos de autenticación. | [ADR-06](decisiones.md#adr-06) (DTO inmutable con actor validado desde token + constraint `NOT NULL`). |
| **5. Contención O(N) por Escaneo Secuencial** | `SELECT ... FOR UPDATE` sin índice condicional recorriendo tablas con miles de históricos. | [ADR-01](decisiones.md#adr-01) (Índice parcial `WHERE status = 'pending'` + `SKIP LOCKED` en O(1)). |

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
func (s *Store) RecordFailure(ctx context.Context, itemID int64, reason string) error {
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

## 2. Incidente FAT-ERR-04: Timeout por Trabajo Lento dentro del Patrón Outbox

### Síntoma
Caída masiva de conexiones del pool de PostgreSQL y timeouts HTTP generalizados (`context deadline exceeded`). Clientes experimentaban latencias superiores a 30 segundos para confirmar lotes pequeños.

### Causa Raíz
En un diseño ingenuo del patrón Transactional Outbox, el endpoint de confirmación abría la transacción de base de datos y, antes de cerrarla con `COMMIT`, invocaba sincrónicamente una llamada HTTP al upstream externo para "pre-validar" el lote. 

Cuando el upstream experimentaba congestión o demoraba 15-20 segundos en responder, la conexión de PostgreSQL permanecía abierta manteniendo bloqueos de fila (`row locks`). Con 20 peticiones concurrentes, el pool de conexiones (`pgxpool.Pool`) se agotaba por completo, estrangulando tanto las lecturas como el resto de las operaciones del sistema.

### Remediación de Ingeniería
* **Regla de Oro del Outbox:** *Una transacción de base de datos NUNCA debe cruzar la frontera de red hacia un servicio externo.*
* La confirmación del lote persiste en PostgreSQL los ítems y el evento en la tabla `outbox` en **una sola transacción atómica que se completa en menos de 25 ms** (verificado en benchmarks: 22 ms para 300 ítems mediante `COPY`).
* Una vez liberada la conexión a la base de datos, los workers de fondo leen el outbox y despachan las llamadas HTTP de forma completamente asíncrona.

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
2. **Health Checks Activos y Fail-Fast:** Se implementó una verificación de salud cada 60 segundos que ejecuta un ping a la base de datos y al upstream. Si un worker falla 3 ciclos consecutivos o pierde sus dependencias esenciales, aborta con `os.Exit(1)` para que el orquestador (Docker / Kubernetes / systemd) destruya el contenedor y lo reinicie con un estado limpio.

---

## 4. Incidente FAT-ERR-03: Registros de Auditoría Huérfanos por Pérdida de Contexto en Go

### Síntoma
La tabla `audit_log` y los eventos en `outbox` presentaban filas con la columna `actor` vacía o nula (`''`), impidiendo la trazabilidad forense requerida para auditorías de cumplimiento.

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
    ID              int64     `json:"id"`
    Actor           string    `json:"actor"` // Obligatorio, extraído del token en el middleware
    TargetWarehouse string    `json:"target_warehouse"`
    CreatedAt       time.Time `json:"created_at"`
}
```
2. **Restricción Estricta en DDL (PostgreSQL):**
```sql
ALTER TABLE batch ADD CONSTRAINT chk_batch_actor_not_empty CHECK (length(trim(actor)) > 0);
ALTER TABLE outbox ADD CONSTRAINT chk_outbox_actor_not_empty CHECK (length(trim(actor)) > 0);
```
3. **Mecanismo Seguro de Desacople de Contexto:** Si se requiere un contexto no cancelable pero con metadatos preservados, se utiliza una estructura que herede valores pero desacople el canal de cancelación (`context.WithoutCancel` en Go 1.21+).

---

## 5. Incidente FAT-ERR-05: Degradación O(N) por Escaneo Secuencial bajo Concurrencia

### Síntoma
A medida que la tabla `batch_item` crecía (superando los 10.000 ítems históricos ya procesados), el tiempo de reclamo (*claim*) de los workers pasó de 30 ms a más de 4.5 segundos por ciclo, disparando la utilización de CPU al 100%.

### Causa Raíz
La consulta de extracción original ejecutaba:
```sql
-- ANTIPATRÓN: Requiere escaneo secuencial de toda la tabla histórica
SELECT id, batch_id, order_ref, priority
FROM batch_item
WHERE status = 'pending'
ORDER BY priority DESC, id ASC
FOR UPDATE SKIP LOCKED
LIMIT 50;
```
Sin un índice especializado, PostgreSQL debía recorrer secuencialmente miles de filas con `status = 'applied'` o `'dlq'` para localizar las pendientes. Bajo concurrencia de múltiples workers paralelos, los bloqueos compartidos en los búferes de disco (*buffer pin contention*) provocaban colisiones y demoras extremas.

### Remediación de Ingeniería
* **Índice Parcial Condicional:** Se creó un índice que indexa **exclusivamente las filas pendientes**, ignorando el histórico ya finalizado:
```sql
CREATE INDEX idx_batch_items_pending_claim 
ON batch_item (priority DESC, id ASC) 
WHERE status = 'pending';
```
* **Impacto Medido:** 
  - Tamaño del índice: reducción del 95% respecto a un índice B-tree completo.
  - Tiempo de despacho: **constante O(1) de ~2 ms** incluso con cientos de miles de registros históricos en la tabla.
  - Cero contención entre workers gracias a `SKIP LOCKED`.

---

## Conclusión

El diseño de `pg-durable-queue` no es un ejercicio teórico: cada cláusula SQL (`SKIP LOCKED`, índices parciales condicionales, `fencing tokens`), cada struct inmutable en Go y cada política de reintento con backoff y jitter existe para **anular de raíz uno de estos 5 modos de falla comprobados en producción**.

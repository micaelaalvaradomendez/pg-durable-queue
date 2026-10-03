# Decisiones técnicas

## ADR-01

**PostgreSQL como cola en lugar de Kafka o Redis.**

- **Contexto:** lotes de hasta 300 ítems, decenas de lotes por día, y el estado del lote ya vive en PostgreSQL.
- **Decisión:** usar `SELECT ... FOR UPDATE SKIP LOCKED` sobre `batch_item`. Cada worker toma filas que nadie más tiene bloqueadas, sin esperar ni duplicar.
- **Consecuencias:** la cola es transaccional con el resto del modelo (no hay dos fuentes de verdad) y no hay otra pieza de infraestructura que operar. Si el volumen creciera órdenes de magnitud o aparecieran varios consumidores del mismo evento, el outbox ya escrito permite sumar un broker sin rediseñar.

## ADR-02

**La confirmación es una sola transacción.**

- **Contexto:** en la versión ingenua, un lote podía quedar creado sin ítems, o con el buffer todavía activo, si algo fallaba en el medio.
- **Decisión:** en una transacción se crean el lote y sus ítems (`COPY`), se da de baja lógica el buffer y se escribe el evento `batch.confirmed` en `outbox`. La respuesta es inmediata: el procesamiento lo hacen los workers.
- **Consecuencias:** o existe todo o no existe nada. La `Idempotency-Key` del lote es `UNIQUE`: repetir la confirmación devuelve el mismo lote, incluso con solicitudes simultáneas.

## ADR-03

**Idempotencia de cada movimiento remoto, no solo del lote.**

- **Contexto:** que el lote sea idempotente no impide duplicar un movimiento en el sistema externo. El caso peligroso es un timeout o una conexión cortada **después** de que el upstream aplicó el cambio.
- **Decisión:** dos capas.
  1. Cada llamada envía `Idempotency-Key` igual al id del ítem, estable entre reintentos: si el upstream ya lo aplicó, responde sin repetir el efecto.
  2. Ante una falla ambigua, antes de reintentar, el worker consulta `GET /assignments/{key}`. Si ya estaba aplicado, lo marca `applied` con resultado `already_applied`.
- **Consecuencias:** 0 duplicados en las pruebas y en la simulación. Si el upstream real no soportara claves de idempotencia, la consulta de estado sigue siendo la segunda barrera, y debería acordarse con el proveedor.

## ADR-04

**Lease con vencimiento y token de cercado.**

- **Contexto:** un worker puede morir con ítems tomados. Sin vencimiento quedarían bloqueados para siempre.
- **Decisión:** al reclamar, el ítem pasa a `in_progress` con `lease_until`. Un ítem con lease vencido vuelve a ser reclamable. Cada reclamo incrementa `attempts`, y el resultado solo se graba si `attempts` sigue siendo el mismo (`WHERE attempts = $n AND status = 'in_progress'`).
- **Consecuencias:** un worker "zombi" que despierta tarde no puede pisar el resultado del worker que tomó el ítem después. El lease debe ser mayor que el timeout del upstream.

## ADR-05

**Reintentos con backoff exponencial y jitter, tope de intentos y DLQ.**

- **Contexto:** las notas originales mezclaban políticas incompatibles (50 reintentos en 5 minutos vs. 5 intentos con espera de minutos).
- **Decisión:**
  - Errores transitorios (5xx, 429, timeouts, conexión cortada): reintento con espera aleatoria en `[d/2, d]`, donde `d = min(máximo, base × 2^(intento-1))`.
  - Tras `MaxAttempts` (5 por defecto) el ítem va a la DLQ.
  - Errores permanentes (4xx): directo a la DLQ, sin reintentar.
- **Consecuencias:** el jitter evita que un lote entero que falló junto reintente en el mismo instante. "Completado" significa que todos los ítems están en un estado terminal (`applied` o `dlq`); los de la DLQ requieren una decisión humana y su reproceso queda auditado (`item.requeued`).

## ADR-06

**El actor sale del token autenticado.**

- **Contexto:** un borrador original tomaba el usuario de una cabecera enviada por el cliente, lo que permite suplantar a cualquiera.
- **Decisión:** el middleware resuelve el actor a partir del token (comparación en tiempo constante) y lo pone en el contexto. El cuerpo rechaza campos desconocidos, así que no se puede enviar un `actor`.
- **Consecuencias:** cada evento del outbox tiene un autor confiable. En producción el token sería OIDC; el contrato con los handlers no cambia.

## ADR-07

**Límites explícitos.**

- **Decisión:** 300 ítems por lote (rechazo sin efectos parciales), 50 por ciclo de worker (configurable), cuerpos HTTP de 16 KiB como máximo y timeouts en el servidor HTTP.
- **Consecuencias:** el peor caso de una transacción de confirmación está acotado y medido (22 ms para 300 ítems en la simulación local).

# Arquitectura MVP de volúmenes remotos para máquinas virtuales (v5)

## EROFS, CoW por bloques, vhost-user-blk, PostgreSQL y WAL durable en S3/RustFS — con fencing formal, cifrado y diseño operativo

**Cambios respecto a v4**

- **Fencing formal** con leases sobre reloj monotónico, espera de promoción y CAS del objeto epoch en S3. Cierra la ventana de pérdida de writes confirmados por un stale writer (era la debilidad de correctness más grave de v4).
- **S3 es la autoridad de recovery**; PostgreSQL pasa a ser caché/control. Nueva regla formal del punto durable + summary objects para acelerar recovery.
- **Término (`term`) verificado en cada transacción del Control Plane**; el advisory lock queda solo como optimización.
- **Cifrado en reposo por volumen** (AES-256-GCM, DEK/KEK, crypto-shredding). Campos reservados en todos los formatos desde el día 1.
- **WAL con extents reales del guest** (offset + length); los 64 KiB quedan solo como granularidad de segmentos CoW. Elimina la amplificación 4→64 KiB del data path.
- **Batching bajo demanda**: se elimina la regla de cierre por 50 ms. PUT solo ante FLUSH/FUA pendiente, tamaño objetivo o antigüedad cercana al límite. Reduce la cardinalidad de objetos en ~2-3 órdenes de magnitud para workloads sin fsync frecuente.
- **Compactación de WAL objects** en background y **GC mark-and-sweep sin permiso de borrado directo** (S3 Versioning + Object Lock + lifecycle).
- **Snapshots crash-consistent sin pausa** (captura atómica de sequence, sellado en background). Freeze del guest opcional, no default.
- **Reconexión vhost-user con inflight tracking** promovida al roadmap temprano (deploys del Agent sin reiniciar VMs).
- **Requisito mínimo de durabilidad del backend de objetos** en on-prem; single-node prohibido en producción.
- **Soporte de DISCARD/WRITE_ZEROES, resize online (grow) y aplanado de cadenas de snapshots**.
- **Clases de I/O internas** (foreground / flush / background) con presupuestos; background siempre cede.
- **Cliente S3 como subsistema**: hedged GETs, retry budget global, límites de ancho de banda por clase.
- **Deterministic Simulation Testing (DST)** como estrategia de verificación principal; interfaces simulables (reloj, red, disco, S3) obligatorias desde el primer commit.
- **Modelo de reconciliación** (desired/current state) para todas las operaciones del Control Plane; cordon/drain de hosts; contabilidad de capacidad.
- **Reconstrucción total de metadata desde S3** (`rebuild-metadata`): layout autodescriptivo con descriptors y manifests con parentesco completo.
- **Tracing distribuido** sobre `request_id`/`operation_id` (OpenTelemetry) y logging estructurado.
- **Política de versionado de formatos y fleet mixto** (read-old ilimitado, write-new gated por feature flag).
- Nueva sección de **caso de uso y SLOs objetivo**, para que las decisiones "aceptadas en el MVP" sean evaluables.

> Nota de verificación: los detalles de features externas citados en este documento (CAS con `If-Match`/`If-None-Match` en S3, MinIO y RustFS; precios de requests de S3) deben confirmarse contra las versiones concretas antes de congelar cada fase. QEMU queda **pineado en 11.0.2** (línea 11.0 liberada en abril de 2026; el protocolo vhost-user incluye `VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD` para recuperar I/O en vuelo tras crash/restart del backend). CI usa exactamente la misma versión de QEMU que producción.

**Cambios v5.1 (decisiones de construcción — esto se construye, no se sigue recortando alcance)**

- **Modo de durabilidad dual por volumen** (`remote` | `local`): en `local`, el ACK de FLUSH ocurre tras `fdatasync` local y S3 es asíncrono con RPO acotado y **medido**. Los snapshots mantienen semántica idéntica en ambos modos (siempre completos en S3). Ver §14.8.
- **Lease por volumen como fase transitoria** (fleets < 50–100 hosts), con renovación **agrupada por host en una sola transacción** desde el día 1. La regla de ACK con reloj monotónico (§12.2) es idéntica en ambas fases. Migración a lease por host = colapso de esquema, no cambio de protocolo.
- **Object Lock diferido**: versioning + lifecycle desde el día 1; el GC sin permisos de borrado permanente desde el día 1 (delete markers reversibles); Object Lock governance como gate antes del primer dato de producción real.
- **DST focalizado**: interfaces simulables + property tests del WAL + checkers de invariantes en el harness desde el commit 1; el set obligatorio de escenarios (fencing, partición, PUT perdido) verde en cada PR. El volumen de semillas crece incrementalmente.
- QEMU pineado en 11.0.2; apalancamiento en librerías maduras (roaring bitmaps, cliente S3 con pooling, OpenTelemetry).

---

## 1. Resumen ejecutivo

El MVP expone tres dispositivos a cada VM:

```text
/dev/vda  EROFS read-only       Imagen base compartida
/dev/vdb  ext4 persistente      Estado durable de la VM
/dev/vdc  ext4 efímero          Docker, containerd y caches
```

La arquitectura utiliza:

- QEMU y `virtio-blk` dentro del guest (con `VIRTIO_BLK_F_FLUSH` y `VIRTIO_BLK_F_DISCARD`).
- `vhost-user-blk` entre QEMU y el Volume Agent, **con reconexión e inflight tracking**.
- Un Volume Agent por host.
- Un único volumen persistente por VM.
- Un volumen efímero local por VM.
- Copy-on-Write con **segmentos de granularidad 64 KiB**; el WAL registra **extents reales** del guest.
- WAL local append-only sobre NVMe.
- WAL remoto agrupado en objetos inmutables de **8 MiB objetivo**, subidos **bajo demanda** (no por timer).
- **Cifrado AES-256-GCM por volumen** de todos los payloads que salen del host.
- PostgreSQL para metadata, ownership, leases, epochs, snapshots y operaciones de control (modelo de **reconciliación**).
- S3 o backend S3-compatible como **frontera durable, backend portable y autoridad de recovery**.
- Objectization, compactación y checkpoints en background, con presupuesto de I/O.
- Preferencia por ejecutar clones en el host donde se creó el snapshot; **standby tibio** por volumen para acotar RTO.

No existe replicación directa entre hosts.

```text
Host A ──X── Host B
```

Cada Volume Agent se comunica directamente con:

- El Control Plane para operaciones administrativas y **renovación de lease**.
- S3 para WAL durable, imágenes, segmentos, checkpoints, manifests y descriptors.

PostgreSQL y el Control Plane no transportan datos de las VMs.

> PostgreSQL coordina quién puede escribir (leases + epochs). S3 determina qué datos son durables, portables y **cuál es la verdad en recovery**. El Agent solo confirma durabilidad al guest mientras su lease está vigente según su reloj monotónico local.

---

## 2. Caso de uso y SLOs objetivo (nuevo)

Las decisiones "aceptadas en el MVP" solo son evaluables contra un caso de uso declarado. El MVP se diseña para:

**Caso de uso primario**: flotas de VMs de desarrollo, CI y entornos efímeros con clonado frecuente desde snapshots (boot rápido same-host, imágenes base compartidas, Docker sobre efímero). Persistencia real pero con tolerancia a RPO write-back estándar.

**SLOs objetivo del MVP** (medidos, no prometidos contractualmente):

| Métrica | Objetivo V1 |
|---|---|
| **RPO** | **una sesión.** Un FLUSH/FUA ACKeado es durable frente a la caída del proceso, del Agent y de QEMU — no frente a la pérdida del host. |
| Latencia de FLUSH | la de `fdatasync` local. S3 no está en el camino del ACK. |
| Pausa de I/O por snapshot | ~0 (crash-consistent, sin quiesce; el punto congelado es un número de secuencia, §19) |
| Pausa de I/O por deploy/crash del Agent | segundos (reconexión vhost-user), sin reinicio de VM |
| Boot de un clon en el host de origen | sin descarga (§20) |
| Boot de un clon en otro host | descarga completa; medido por GiB, no prometido |

**El RPO de una sesión es una decisión, no una limitación pendiente de arreglar** (ADR-0026). Un host que muere a mitad de sesión pierde todo lo escrito desde que el volumen se atachó. Se documenta explícitamente al usuario, sin letra chica.

Es coherente con el caso de uso de arriba: nadie pide que un runner de CI sobreviva a la muerte de su host. La versión anterior de esta tabla pedía **RPO 0 bajo el modelo de fallas probado por DST**, y esa fila —no el caso de uso— es la que generaba la cadena de durabilidad remota completa: subida por cada FLUSH, ACK gobernado por lease, checkpoints, promoción y recuperación a mitad de sesión. Nadie la había pedido.

Un volumen que deba sobrevivir a la pérdida del host es **V2**, y vuelve con un requisito real detrás.

---

## 3. Objetivos del MVP

1. Boot con EROFS, persistente y efímero.
2. Docker/containerd únicamente sobre el disco efímero.
3. Read, write, flush, FUA y **discard** mediante `vhost-user-blk`.
4. Snapshots inmutables **sin pausa de I/O**.
5. Clones independientes.
6. Clone en el mismo host sin descargar nuevamente el snapshot.
7. Recovery desde checkpoint y WAL remoto, **con S3 como autoridad**.
8. Fencing de stale writers mediante **lease + epoch**, sin ventana de pérdida de writes confirmados.
9. Requests duplicados idempotentes.
10. Backpressure antes de llenar NVMe (límites duros de unflushed).
11. **DST + fault injection** automatizados en CI.
12. **Cifrado en reposo** de todo dato de VM fuera del host.
13. **Reconexión vhost-user**: crash o deploy del Agent no reinicia VMs.
14. Resize online (grow) del volumen persistente.
15. Operación local, on-premises o cloud sin Kubernetes.

### No objetivos iniciales

- Multi-writer.
- Live migration.
- Comunicación directa entre nodos.
- Journal Raft / consenso propio.
- Réplica síncrona host-to-host.
- Multi-queue.
- `io_uring`.
- Lazy loading del volumen persistente (diseñado, no implementado; ver §22).
- Shrink de volúmenes.
- Scheduler avanzado / QoS entre tenants (sí hay clases de I/O internas, ver §11).
- Arquitectura global multi-región.

---

## 4. Decisiones principales

| Área | Decisión MVP |
|---|---|
| Control Plane | Servicio single-active con `term` verificado por transacción |
| Metadata | PostgreSQL (caché/control; **no** autoridad de recovery) |
| Autoridad de recovery | **S3** (prefijo contiguo del epoch fenced más alto) |
| Interfaz guest | virtio-blk (FLUSH + DISCARD anunciados; write-back explícito) |
| Backend QEMU | vhost-user-blk **con reconexión + inflight shmfd** |
| Imagen base | EROFS |
| Volumen durable | Un ext4 persistente (grow online soportado) |
| Datos reconstruibles | Un ext4 efímero |
| Writer | Single writer |
| **Modo de durabilidad (por volumen)** | **`remote` (default: FLUSH→S3, RPO 0) \| `local` (FLUSH→fdatasync local; S3 async, RPO ≤ unflushed_age, medido)** |
| Fencing | Lease + epoch + espera de promoción (TTL + skew); regla de ACK idéntica en ambos modos de lease |
| Granularidad del lease | **Fase A (< 50–100 hosts): por volumen, renovación agrupada por host. Fase B: por host (§12.6)** |
| Cota de deriva de reloj asumida | **2 s** (NTP/chrony obligatorio y monitoreado) |
| Cifrado | **AES-256-GCM por volumen; DEK por volumen, KEK en KMS/Vault** |
| WAL local | Append-only en NVMe |
| WAL remoto | Objetos inmutables en S3 |
| Registro de WAL | **Extent real del guest (offset + length, alineado a 512 B)** |
| Granularidad CoW (segmentos) | 64 KiB |
| Tamaño objetivo de WAL object | 8 MiB |
| Tamaño máximo de WAL object | 16 MiB |
| Cierre/PUT de batch | **Bajo demanda**: FLUSH/FUA pendiente, ≥ 8 MiB, o antigüedad ≥ 20 s |
| Compactación de WAL objects | Background, objetivo 64–128 MiB por objeto compactado |
| Segmento CoW objectized | 128 MiB objetivo |
| ACK de WRITE normal | Después de append local |
| ACK de FUA/FLUSH | **Verificación de lease vigente** + `fdatasync` local + PUT remoto verificado |
| Snapshot | Crash-consistent, captura atómica de sequence, sin pausa; freeze opcional |
| Profundidad máxima de cadena | 5 niveles → aplanado en background |
| Queues | Una |
| Queue depth | 128 |
| `io_uring` | Pospuesto |
| Cross-host | S3 (materialización completa en MVP; standby tibio para volúmenes críticos) |
| Failover | Controlado, no automático; RTO publicado en runbook |
| Máximo unflushed por volumen | 1 GiB (configurable) |
| Máxima antigüedad unflushed | 30 s (configurable) |
| Checkpoint interval (MVP) | 256 MiB de WAL o 2 minutos |
| Checkpoint on snapshot | true (en background, no bloquea el snapshot) |
| Buckets | **Día 1: versioning + lifecycle; GC sin borrado permanente. Object Lock (governance): gate antes de producción real** |
| QEMU | **Pineado: 11.0.2 (misma versión exacta en CI y producción)** |
| Backend on-prem | **Replicación/EC obligatoria (tolerar ≥ 1 nodo caído); single-node prohibido en prod** |
| Testing | **DST determinístico + fault injection + property tests del WAL** |

---

## 5. Invariantes

### 5.1 Single writer con lease

Existe como máximo un writer válido por volumen. Toda operación mutable incluye:

```text
volume_id
epoch
sequence
operation_id
```

Un Agent con epoch anterior no puede publicar checkpoints, manifests ni actualizar metadata (CAS + validación en PG).

**Nuevo — regla de ACK durable**: el Agent solo ACKea un FLUSH/FUA si, según su **reloj monotónico local**, su lease sigue vigente en el instante del ACK. Un PUT exitoso a S3 con lease vencido **no** se confirma al guest.

**Nuevo — regla de promoción**: el Control Plane no promueve un nuevo writer hasta que transcurrió `lease_ttl + max_clock_skew` desde el último heartbeat aceptado del writer anterior. Solo entonces el nuevo Agent fija su punto de recovery.

Estas dos reglas, juntas, garantizan que ningún write ACKeado como durable puede quedar fuera del prefijo que ve el nuevo writer. (Ver §12 para el protocolo completo.)

### 5.2 Snapshots inmutables

Después de publicar un snapshot: su root no cambia, sus WAL objects no cambian, sus segmentos no cambian. Cada clone crea un active child independiente. La compactación puede **reemplazar** objetos por equivalentes verificados byte-a-byte en contenido lógico, nunca cambiar el contenido lógico referenciado por un manifest.

### 5.3 S3 no participa en cada WRITE

El dispositivo anuncia write-back cache al guest (`VIRTIO_BLK_F_FLUSH`). Un WRITE normal se completa tras persistir localmente. La garantía frente a pérdida del host se obtiene cuando el write está cubierto por un `FLUSH` exitoso, un WRITE con FUA, o un snapshot publicado.

### 5.4 Localidad no es durabilidad

La caché local acelera el arranque, pero no reemplaza S3.

### 5.5 El efímero puede perderse

Nada contractualmente durable debe depender de `/dev/vdc`.

### 5.6 Watermarks ordenados

```text
published_sequence <= durable_sequence <= local_sequence
```

### 5.7 Límites de WAL local no flushed

```text
unflushed_bytes <= max_unflushed_bytes_per_volume   (default 1 GiB)
oldest_unflushed_age <= max_unflushed_age           (default 30 s)
```

Si se superan, se aplica backpressure: se rechazan nuevos WRITEs con error explícito al guest.

### 5.8 S3 es la autoridad de recovery (nuevo)

> El punto durable de un volumen = el final del **prefijo contiguo más largo** de sequences bajo `wal/<vol>/<epoch>/` para el **epoch fenced más alto**. Los watermarks en PostgreSQL son informativos (actualización lazy), nunca autoridad.

Corolario: PostgreSQL completo debe poder reconstruirse desde S3 (`rebuild-metadata`, §22.5). El layout en S3 es autodescriptivo.

### 5.9 Background siempre cede (nuevo)

Todo I/O del Agent pertenece a una clase (foreground / flush / background). El tráfico background (objectization, compactación, hidratación, prefetch, GC) opera con presupuesto fijo de NVMe y red, y cede ante foreground y flush. Ninguna mejora de background puede degradar la latencia del data path.

### 5.10 Nada sale del host en claro (nuevo)

Todo payload de datos de VM (WAL records, segmentos, checkpoints) se cifra con la DEK del volumen antes de cualquier PUT. Los metadatos estructurales (headers, manifests, descriptors) no contienen datos del guest.

### 5.11 El GC no puede causar el peor incidente (nuevo)

El GC marca; nunca ejecuta borrado permanente. El borrado real lo ejecuta el lifecycle del bucket sobre versiones no-actuales tras el grace period. Las credenciales del GC no incluyen `DeleteObject` permanente ni bypass de Object Lock.

---

## 6. Arquitectura de deployment

(Ver diagramas `diagrama_deployment_mvp.svg` y `diagrama_nodo_y_flujos_mvp.svg`; actualizar en v5 con: lease heartbeats CP↔Agent, KMS, standby tibio.)

### Comunicación

| Origen | Destino | Protocolo | Uso |
|---|---|---|---|
| Cliente | Control Plane | gRPC/HTTP | Attach, snapshot, clone, resize |
| Control Plane | PostgreSQL | PostgreSQL/TLS | Metadata, leases, epochs, reconciliación |
| Control Plane | Volume Agent | gRPC/mTLS | Operaciones administrativas |
| Volume Agent | Control Plane | gRPC/mTLS | **Heartbeat + renovación de lease + reporte de capacidad** |
| Volume Agent | KMS/Vault | HTTPS/mTLS | Unwrap de DEKs (solo en attach/recovery) |
| QEMU | Volume Agent | vhost-user Unix socket (reconectable) | I/O |
| Volume Agent | S3 | HTTPS/S3 API | WAL, checkpoints, imágenes, descriptors |
| Host | Otro host | Ninguno | No existe replicación directa |

### mTLS y credenciales

- Rotación de certificados mTLS **automatizada desde el día 1** (proceso, no TODO). Alerta a 30 días del vencimiento.
- Credenciales S3 por Agent scopeadas al mínimo: prefijos de datos, sin permisos de administración de bucket, sin bypass de Object Lock.
- Credenciales del GC: solo marcar (tags / delete markers versionados), nunca borrado permanente.

### 6.1 Requisitos del backend de objetos (nuevo)

La durabilidad real del sistema es la durabilidad del backend. Requisitos:

**Cloud**: S3 (u equivalente con durabilidad publicada). Versioning + Object Lock (governance) + lifecycle en buckets de producción.

**On-premises (producción)**: el backend DEBE tolerar la pérdida de al menos un nodo completo sin pérdida de datos ni de disponibilidad de escritura:

- Opción robusta: MinIO multi-nodo con erasure coding (mínimo 4 nodos).
- Opción liviana: Garage con factor de replicación 3.
- **Prohibido en producción**: RustFS/MinIO single-node. Permitido únicamente en el modo `local` de desarrollo.

**Suite de conformidad del backend** (bloqueante para habilitar un backend): `If-None-Match: *`, `If-Match` (CAS sobre ETag), HEAD tras PUT "perdido", versioning, Object Lock, comportamiento bajo throttling, y consistencia de LIST tras PUT. Se corre contra cada versión del backend antes de actualizar.

### 6.2 Modos de ejecución

**Local** (desarrollo):

```text
localhost
├── PostgreSQL
├── MinIO/RustFS single-node (solo dev)
├── KMS de desarrollo (KEK en archivo)
├── Control Plane
├── Volume Agent
└── QEMU
```

**On-premises**:

```text
Hosts de control (1 activo + 1 standby)
├── Control Plane
└── PostgreSQL (con PITR hacia el object store)

Object storage (≥ 4 nodos MinIO EC, o ≥ 3 nodos Garage RF=3)

KMS: Vault (o KEK por host con permisos estrictos como mínimo aceptable)

Hosts de cómputo
├── Volume Agent
├── QEMU
├── RAM
└── NVMe local
```

**Cloud**:

```text
Managed PostgreSQL (con PITR)
S3 + KMS
VMs de cómputo: Volume Agent + QEMU
```

Cada componente puede ejecutarse como binario con systemd o como contenedor Docker/Podman.

---

## 7. Control Plane y PostgreSQL

### Control Plane

Responsabilidades:

- Crear, eliminar y **redimensionar** volúmenes.
- Attach y detach.
- Asignar host (con contabilidad de capacidad).
- **Administrar leases y promociones** (incluida la espera de fencing).
- Incrementar epoch.
- Crear snapshots y clones; programar aplanado de cadenas.
- Registrar hosts; **cordon/drain**.
- Preferir el source host; mantener standby tibio para volúmenes marcados.
- Coordinar recovery controlado.
- Ejecutar el reconciler general y el GC (fase de marcado).
- Auditar operaciones.
- Idempotencia administrativa mediante `request_id` (UUID) en **todas** las operaciones mutables.

No participa en el data path.

### Modelo de reconciliación (nuevo)

Toda operación de larga duración (attach, detach, clone, drain, recovery, aplanado, resize) se representa como una fila con `desired_state` / `current_state`. Loops de reconciliación idempotentes y nivel-triggered convergen el estado. El CP puede crashear en cualquier punto: nada queda a medias, solo re-converge. La respuesta operativa ante incidentes es "arreglar la causa y dejar reconciliar", no cirugía manual en la base.

### Single-active con término verificado (reemplaza al advisory lock puro)

El advisory lock queda como optimización para evitar dos CP compitiendo activamente. La corrección la da el término:

```sql
CREATE TABLE control_plane_leader (
    singleton   BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    term        BIGINT NOT NULL,
    holder_id   TEXT NOT NULL,
    renewed_at  TIMESTAMPTZ NOT NULL
);
```

- Al tomar liderazgo: `UPDATE control_plane_leader SET term = term + 1, holder_id = $me, renewed_at = now()`.
- **Toda transacción de escritura del CP** incluye la validación `WHERE term = $mi_term` (o trigger equivalente). Un CP zombie con término viejo afecta 0 filas, detecta la condición y se termina a sí mismo.

Esto cierra la carrera clásica del advisory lock ligado a sesión (conexión caída → lock liberado → dos CP creyéndose activos).

### Failover del writer: estados

```text
ACTIVE
PRIMARY_SUSPECTED
FENCING_WAIT        ← nuevo: esperando lease_ttl + skew
RECOVERY_REQUIRED
RECOVERING
DETACHED
```

No se promueve un nuevo writer solo por un heartbeat vencido: siempre se transita por `FENCING_WAIT`.

### PostgreSQL

Autoridad para: leases, epochs, ownership, attachments, catálogo de snapshots/clones, hosts y capacidad, operaciones de reconciliación, idempotencia administrativa, términos del CP.

**No** es autoridad para el punto durable de los datos (eso es S3, §5.8). No almacena bloques ni WAL payloads.

Operación: PITR obligatorio (WAL-G o pgBackRest hacia el mismo object store), restore **ensayado** y documentado en runbook. Ante pérdida total: `rebuild-metadata` desde S3 (§22.5).

---

## 8. Modelo PostgreSQL mínimo

```sql
CREATE TABLE volumes (
    volume_id          TEXT PRIMARY KEY,
    size_bytes         BIGINT NOT NULL,          -- mutable: resize grow
    durability         TEXT NOT NULL DEFAULT 'remote',  -- 'remote' | 'local' (§14.8)
    block_size         INTEGER NOT NULL,          -- granularidad de segmentos (64 KiB)
    current_epoch      BIGINT NOT NULL DEFAULT 0,
    state              TEXT NOT NULL,
    primary_host_id    TEXT,
    standby_host_id    TEXT,                      -- standby tibio opcional
    active_root_id     TEXT,
    published_root_id  TEXT,
    chain_depth        INTEGER NOT NULL DEFAULT 0,
    dek_wrapped        BYTEA NOT NULL,            -- DEK cifrada con la KEK
    kek_id             TEXT NOT NULL,
    -- Watermarks INFORMATIVOS (lazy). La autoridad es S3 (§5.8):
    local_sequence     BIGINT NOT NULL DEFAULT 0,
    durable_sequence   BIGINT NOT NULL DEFAULT 0,
    published_sequence BIGINT NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
-- Lease POR HOST, no por volumen (escalabilidad, ver §12.6): una renovación
-- cada 3 s por host — no V/3 tx/s con V volúmenes. Cada volumen queda ligado
-- al lease de su host vía (primary_host_id, current_epoch).
CREATE TABLE host_leases (
    host_id       TEXT PRIMARY KEY REFERENCES hosts(host_id),
    granted_at    TIMESTAMPTZ NOT NULL,
    last_renewal  TIMESTAMPTZ NOT NULL,
    ttl_seconds   INTEGER NOT NULL DEFAULT 10
);
```

```sql
CREATE TABLE hosts (
    host_id             TEXT PRIMARY KEY,
    state               TEXT NOT NULL,   -- ACTIVE | CORDONED | DRAINING | DEAD
    agent_version       TEXT NOT NULL,
    max_format_version  INTEGER NOT NULL, -- para fleet mixto (§27)
    nvme_total_bytes    BIGINT NOT NULL,
    nvme_used_bytes     BIGINT NOT NULL,
    nvme_committed_bytes BIGINT NOT NULL, -- thin provisioning comprometido
    last_heartbeat      TIMESTAMPTZ NOT NULL
);
```

```sql
CREATE TABLE snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    volume_id           TEXT NOT NULL REFERENCES volumes(volume_id),
    parent_snapshot_id  TEXT,
    epoch               BIGINT NOT NULL,
    target_sequence     BIGINT NOT NULL,
    root_digest         TEXT NOT NULL,
    source_host_id      TEXT,
    state               TEXT NOT NULL,
    portable            BOOLEAN NOT NULL DEFAULT false,
    manifest_key        TEXT,
    request_id          UUID UNIQUE NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
CREATE TABLE operations (          -- reconciliación general
    operation_id   UUID PRIMARY KEY,      -- = request_id del cliente
    kind           TEXT NOT NULL,         -- attach|detach|clone|resize|drain|recovery|flatten|gc
    volume_id      TEXT,
    host_id        TEXT,
    desired_state  JSONB NOT NULL,
    current_state  JSONB NOT NULL,
    phase          TEXT NOT NULL,
    error          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Política de sobresuscripción de NVMe explícita y configurable (ej. `committed/total <= 2.0`) con alerta.

---

## 9. Layout del guest

```text
/dev/vda  EROFS read-only
/dev/vdb  ext4 persistente
/dev/vdc  ext4 efímero
```

Root:

```bash
mount -t erofs -o ro /dev/vda /sysroot.lower
mount -t ext4 /dev/vdb /sysroot.state

mkdir -p /sysroot.state/root-upper
mkdir -p /sysroot.state/root-work
mkdir -p /sysroot.merged

mount -t overlay overlay \
  -o lowerdir=/sysroot.lower,\
upperdir=/sysroot.state/root-upper,\
workdir=/sysroot.state/root-work \
  /sysroot.merged
```

Efímero:

```bash
mount -t ext4 /dev/vdc /sysroot.merged/var/lib/ephemeral

mount --bind /sysroot.merged/var/lib/ephemeral/docker \
  /sysroot.merged/var/lib/docker

mount --bind /sysroot.merged/var/lib/ephemeral/containerd \
  /sysroot.merged/var/lib/containerd
```

Si el efímero falla, Docker/containerd no inician y no se usa el persistente como fallback silencioso.

Features anunciadas al guest en `/dev/vdb`:

- `VIRTIO_BLK_F_FLUSH`: write-back explícito; el guest sabe qué garantías tiene.
- `VIRTIO_BLK_F_DISCARD` y `VIRTIO_BLK_F_WRITE_ZEROES`: el espacio liberado por el guest se recupera (ver §14.6). Montar ext4 con `discard` o correr `fstrim` periódico.
- Resize: el grow se propaga vía actualización del config space + notificación; el guest expande con `resize2fs`.

---

## 10. Volume Agent

Un proceso por host:

- `vhost-user-blk` **con reconexión e inflight tracking** (`VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD`, modelo SPDK vhost): las requests en vuelo viven en memoria compartida; un crash/deploy del Agent produce una pausa de segundos, no un reinicio de VMs. QEMU reintenta la conexión al socket automáticamente.
- Storage Engine CoW + active block maps (bitmaps comprimidos, ver §13.3).
- WAL local + batcher bajo demanda.
- Motor de cifrado por volumen (§15).
- Cliente S3 como subsistema (§24).
- Caché RAM/NVMe.
- Objectizer + compactador + hidratador de standby (todos clase background).
- Scheduler de clases de I/O (§11).
- Lease manager (reloj monotónico; §12).
- Recovery.
- Métricas + tracing.
- Hooks de DST y fault injection.

Configuración inicial:

```text
queues: 1
queue depth: 128
lease_ttl: 10s                  # lease POR HOST (§12.6)
lease_renewal_interval: 3s      # en el heartbeat de capacidad
max_clock_skew: 2s
max_unflushed_bytes_per_volume: 1 GiB
max_unflushed_age: 30s
batch_target_bytes: 8 MiB
batch_max_bytes: 16 MiB
batch_max_age_for_upload: 20s
checkpoint_interval_bytes: 256 MiB
checkpoint_interval_time: 2m
max_chain_depth: 5
background_net_budget: 30% de NIC (configurable)
background_nvme_budget: 30% de IOPS/BW (configurable)
```

### 10.1 Presupuesto de memoria (nuevo)

El Agent declara y respeta un presupuesto explícito; nada crece sin cota:

```text
por volumen:
  batches abiertos+pendientes:  <= batch_max_bytes * max_pending_batches (ej. 16 MiB * 4)
  active map:                   O(bloques dirty), bitmap comprimido; métrica active_map_bytes
  buffers de cifrado:           reutilizados por pool

global:
  caché RAM:                    tamaño fijo configurable, LRU
  total del Agent:              límite duro; al acercarse → evicción + backpressure, nunca OOM
```

Métricas: `agent_memory_bytes{component}`, `active_map_bytes{volume}`.

---

## 11. Clases de I/O internas (nuevo)

Todo I/O del Agent pertenece a exactamente una clase:

| Clase | Contenido | Política |
|---|---|---|
| **foreground** | Data path del guest: WAL append, `fdatasync`, read misses | Prioridad absoluta en NVMe |
| **flush** | PUTs que bloquean ACKs de FLUSH/FUA | Prioridad absoluta en red |
| **background** | Objectization, compactación, hidratación de standby, prefetch, GC, materialización | Presupuesto fijo (token bucket por recurso); cede siempre |

Implementación mínima: token buckets por clase para NVMe (IOPS + bytes) y para red (bytes), en el propio Agent. Sin esto, cada proceso de background es una fuente de incidentes de latencia; con esto, son invisibles.

Métricas: `io_class_bytes_total{class,resource}`, `io_class_throttled_seconds{class}`.

---

## 12. Fencing y leases (nuevo — protocolo completo)

Este protocolo cierra la ventana de v4 en la que un stale writer con acceso a S3 podía seguir ACKeando FLUSHes que el nuevo writer jamás vería.

### 12.1 Supuestos explícitos

1. Relojes **monotónicos** locales correctos (no requiere sincronía absoluta para la seguridad del writer viejo).
2. Deriva máxima de reloj de pared entre CP y hosts acotada por `max_clock_skew = 2 s`. NTP/chrony obligatorio; alerta si el offset reportado supera 500 ms. Este supuesto solo se usa para la espera de promoción.

### 12.2 Ciclo del lease (lado Agent)

```text
1. El lease es POR HOST: un único lease cubre todos los volúmenes
   attacheados al host (cada uno conserva su propio epoch de attachment).
   El Agent registra t0 = monotonic_now() al recibir la concesión.
2. Renovación cada lease_renewal_interval (3 s) vía el MISMO heartbeat
   que reporta capacidad: una transacción por host, no por volumen.
   Cada renovación exitosa actualiza t0.
3. lease_valido() := monotonic_now() - t0 < lease_ttl
   (gobierna los ACKs durables de TODOS los volúmenes del host)
```

**Regla dura de ACK** (sin round-trips extra; una comparación de timestamps):

```text
Antes de ACKear cualquier FLUSH/FUA:
    if !lease_valido():
        no ACKear; fallar la request; transicionar a SELF_FENCED
Un PUT exitoso con lease vencido NO se confirma al guest.
```

Al vencer el lease sin renovación, el Agent entra en `SELF_FENCED`: deja de ACKear durabilidad, deja de publicar checkpoints/manifests, y espera instrucciones (puede seguir sirviendo reads de su caché mientras QEMU siga conectado, según política).

### 12.3 Promoción (lado Control Plane)

```text
1. Heartbeat del writer W1 (epoch N) vencido → PRIMARY_SUSPECTED.
2. Decisión de failover (manual en MVP) → FENCING_WAIT.
3. Esperar hasta: last_renewal(W1) + lease_ttl + max_clock_skew  (10 + 2 = 12 s)
   Garantía: pasado ese instante, W1 ya no puede ACKear durabilidad
   (por la regla dura de 12.2 sobre su reloj monotónico).
4. Incrementar epoch → N+1. CAS del objeto de epoch en S3 (12.4).
5. Conceder lease del epoch N+1 al nuevo host W2 → RECOVERING.
6. W2 fija su punto de recovery listando S3 (§22.1). Todo write que W1
   ACKeó como durable está, por construcción, dentro de ese prefijo.
```

### 12.4 Cinturón y tiradores: objeto de epoch con CAS en S3

Objeto pequeño `volumes/<volume_id>/epoch` con el epoch vigente. Operaciones de **baja frecuencia** (publicar checkpoint, publicar manifest, compactación) lo validan con escritura condicional (`If-Match` sobre ETag) o al menos GET + comparación antes de publicar. No se aplica a cada PUT de WAL: ahí el lease local alcanza y el CAS duplicaría latencia.

> Verificar soporte de `If-Match`/CAS en la versión desplegada de MinIO/RustFS; S3 lo soporta desde 2024. Si el backend no lo soporta, el protocolo sigue siendo seguro por 12.2 + 12.3; el CAS es defensa en profundidad.

### 12.5 Qué pasa con los PUTs tardíos del writer viejo

Un W1 particionado puede lograr PUTs a `wal/<vol>/<N>/...` después del fencing. Es inocuo:

- No fueron ACKeados al guest (regla 12.2).
- El recovery de W2 fijó su prefijo del epoch N en el paso 6; los objetos tardíos quedan fuera del prefijo elegido o pertenecen a sequences ya cubiertas.
- El GC los recoge como huérfanos (objetos de epoch < vigente no referenciados por el punto de recovery registrado).

Para eliminar ambigüedad, W2 escribe al inicio de su epoch un objeto `wal/<vol>/<N+1>/recovery-point.json` con `{prev_epoch: N, recovered_up_to: seq}`. Ese objeto es la frontera inmutable entre epochs y la referencia del GC.

### 12.6 Escalabilidad del fencing (por qué el lease es por host)

Un lease por volumen renovado cada 3 s genera `V/3` transacciones/s contra PG a través del CP: con 1.000 volúmenes son ~330 tx/s y con 10.000 son ~3.300 tx/s solo de leases — no escala. Con lease por host son `H/3` tx/s: con 300 hosts, ~100 tx/s triviales, independiente de cuántos volúmenes tenga cada host (modelo de agregación tipo GFS). El costo del trade-off: la expiración del lease de un host fencea la durabilidad de *todos* sus volúmenes a la vez — que es exactamente el comportamiento correcto, porque el evento que se está detectando (host particionado o muerto) afecta al host entero. La promoción (`FENCING_WAIT`) también opera a nivel host y luego recupera volumen por volumen.

**Fase transitoria (v5.1, fleets < 50–100 hosts)**: se permite lease por volumen (filas por volumen, TTL 15–30 s, renovación cada 5–10 s) mientras se valida el resto del sistema, con tres condiciones no negociables:

1. La **renovación va agrupada por host** en un solo RPC/transacción desde el día 1 (el heartbeat ya existe; es una línea). Con esto la migración a lease por host es un colapso de esquema, no un cambio de protocolo.
2. La **regla de ACK con reloj monotónico (§12.2) es idéntica** en ambas fases — es lo que hace el fencing correcto, no el esquema de la tabla.
3. Acoples asumidos explícitamente: `TTL ≥ 3× intervalo de renovación`; `FENCING_WAIT = TTL + skew` sube en proporción (15–30 s + 2 s), y la ventana de degradación con PG caído para volúmenes `remote` también.

Misma disciplina para los watermarks informativos: las actualizaciones lazy de `local/durable/published_sequence` se agrupan **por host en una sola transacción periódica** (ej. cada 15–30 s), no una tx por volumen.

---

## 13. Modelo CoW

### 13.1 Granularidades desacopladas (cambio clave vs v4)

- **WAL**: registra el **extent real** del write del guest (offset + length, alineado a 512 B). Un write de 4–16 KiB loguea 4–16 KiB, no 64 KiB. El read-modify-write desaparece del data path.
- **Segmentos CoW**: granularidad de 64 KiB. El RMW se paga una sola vez, en objectization (clase background), donde no afecta latencia del guest.

Esto reduce simultáneamente: bytes de WAL local, bytes subidos a S3, tiempo de replay y costo — precisamente para el peor workload (bases de datos con páginas de 8–16 KiB y fsync frecuente).

### 13.2 Read path

```text
dirty local (extents recientes en WAL/caché)
→ active map (segmentos 64 KiB)
→ checkpoint
→ segmento local
→ S3
→ zero block
```

Los extents del WAL aún no objectizados se indexan en memoria (interval map por volumen) para resolver reads con overlap parcial. En el MVP, un attach cross-host materializa el snapshot completo antes del boot (mitigado por standby tibio, §22.3; lazy loading diseñado para después, §22.4).

### 13.3 Active map con cota de memoria

Con segmentos de 64 KiB, 1 TiB = 16M entradas posibles. Implementación: roaring bitmaps (presencia) + tabla de ubicaciones por rangos, no hashmaps naive. Métrica `active_map_bytes` por volumen; entra en el presupuesto de memoria del Agent (§10.1).

---

## 14. Formato exacto del WAL y reglas de batching (v2)

Pieza más crítica de correctness del data path. Cambios v5: extents reales, tipos de record (write/discard/write_zeroes), campos de cifrado, y reglas de cierre/PUT bajo demanda.

### 14.1 WAL Record

```go
// Header de tamaño fijo: 104 bytes (erratum v5.1: los campos enumerados suman 104,
// no 96 — ver ADR-0005 / DEV-0001. El "96" es anterior a los campos de cripto de v5.)
type BlockWriteRecordHeader struct {
    Magic         [4]byte   // "VW02"
    Version       uint16    // 2
    HeaderLen     uint16    // 104
    RecordType    uint8     // 0=WRITE, 1=DISCARD, 2=WRITE_ZEROES
    Reserved0     [3]byte
    VolumeID      [16]byte  // UUID
    Epoch         uint64
    Sequence      uint64    // monotónico por (volume_id, epoch)
    OperationID   [16]byte  // UUID del request (idempotencia / tracing)
    Offset        uint64    // offset en bytes dentro del volumen (alineado a 512 B)
    Length        uint32    // longitud del extent (0 payload para DISCARD/WRITE_ZEROES)
    KeyID         uint32    // versión de la DEK usada
    PayloadCRC32C uint32    // CRC32C del payload EN CLARO (verificación post-descifrado)
    Flags         uint32    // bit 0 = FUA; bit 1 = parte de FLUSH
    AuthTag       [16]byte  // GCM tag del payload cifrado
    HeaderCRC32C  uint32
    // Payload [Length]byte cifrado AES-256-GCM
    // nonce derivado = KDF(volume_id, epoch, sequence) → no se almacena
}
```

- `Sequence` estrictamente monotónico por `(volume_id, epoch)`.
- DISCARD/WRITE_ZEROES son records sin payload: baratos, y esenciales para que el storage converja al working set (§14.6).
- El CRC en claro + el tag GCM dan verificación en dos capas: integridad criptográfica del objeto y validación del pipeline de descifrado.

### 14.2 WAL Object (batch remoto)

```go
// Header de tamaño fijo: 104 bytes (mismo erratum que §14.1 — ADR-0005 / DEV-0001).
type WALObjectHeader struct {
    Magic          [4]byte   // "WB02"
    Version        uint16    // 2
    HeaderLength   uint16    // 104
    VolumeID       [16]byte
    Epoch          uint64
    FirstSequence  uint64
    LastSequence   uint64
    RecordCount    uint32
    KeyID          uint32
    PayloadLength  uint64    // bytes totales (headers + payloads cifrados)
    PayloadSHA256  [32]byte  // SHA-256 del payload concatenado (tal como se sube)
    HeaderCRC32C   uint32
    Reserved       [4]byte
}
```

**Clave S3 determinística e idempotente:**

```text
wal/<volume_id>/<epoch>/<first_seq>-<last_seq>-<sha256_prefix8>.wal
```

### 14.3 Reglas de cierre y PUT de batch (reemplaza la regla de 50 ms)

La durabilidad solo se promete en FLUSH/FUA; no hay razón para subir batches por timer corto. Un batch se cierra y se programa para PUT cuando se cumple **cualquiera** de:

1. **FLUSH o FUA recibido**: cierre inmediato (aunque tenga 1 record). Los PUTs pendientes se suben **en paralelo** (pipeline bajo demanda).
2. **Tamaño objetivo**: `current_batch_bytes >= 8 MiB`.
3. **Tamaño máximo**: `>= 16 MiB` (hard limit).
4. **Antigüedad**: primer record con más de **20 s** (colchón contra `max_unflushed_age = 30 s`).
5. **Snapshot solicitado**: cierre y durable hasta `target_sequence` (en background, §19).
6. **Límite de unflushed alcanzado**: cierre forzado para liberar presión.
7. **Shutdown limpio**.

Efecto: un volumen sin fsyncs frecuentes pasa de ~20 PUTs/s (v4) a ~3 PUTs/min. La cardinalidad de objetos y el costo de requests bajan 2-3 órdenes de magnitud. Métricas `wal_batch_size_bytes` y `wal_small_batch_ratio` vigilan el caso fsync-pesado, que se corrige con compactación (§21.2).

### 14.4 Orden de operaciones en FLUSH / FUA (crítico)

```text
1. Capturar target_sequence = local_sequence actual
2. Cerrar el batch actual (si tiene datos)
3. fdatasync() de los archivos WAL locales con sequences <= target
4. Para cada batch pendiente con LastSequence <= target (EN PARALELO):
     a. SHA-256 del payload → construir header
     b. PUT con If-None-Match: *
     c. Si 412 → HEAD; si tamaño + SHA-256 coinciden → éxito idempotente
     d. Si no coinciden → corrupción/divergencia → fallo duro
5. VERIFICAR lease_valido() según reloj monotónico local
     → si venció: NO ACKear; SELF_FENCED
6. Actualizar durable_sequence = target_sequence
7. ACK al guest
```

Nota de semántica FUA: virtio solo exige durabilidad del write marcado, no de todos los anteriores; implementarlo como flush-hasta-target es correcto pero pesimista. Se acepta en el MVP por simplicidad; queda documentado como optimización futura (flush selectivo del extent FUA).

### 14.5 Idempotencia de PUT

Idéntica a v4: clave determinística por `(volume_id, epoch, first_seq, last_seq, content_hash)`, `If-None-Match: *`, y retry + HEAD + checksum ante respuesta perdida. Mismo rango con hash distinto → fallo duro.

### 14.6 DISCARD y recuperación de espacio (nuevo)

Sin TRIM, los bloques borrados por el guest viven para siempre: el costo de S3 solo crece y la materialización cross-host descarga basura. Con DISCARD:

- El objectizer marca los segmentos/rangos descartados como ausentes en el active map.
- Los checkpoints y la compactación no reescriben rangos descartados.
- Los reads de rangos descartados devuelven ceros.

El storage converge al working set en lugar de crecer monotónicamente. Métrica: `discarded_bytes_total`.

### 14.7 Layout local del WAL

```text
/var/lib/volume-agent/volumes/<volume-id>/
├── state.json                  # epoch, sequences, lease info, roots
├── wal/
│   ├── <epoch>-<first_seq>.wal
│   └── active.wal
├── cache/
├── segments/
└── checkpoints/
```

Rotación a ~256 MiB o en checkpoint. Nunca truncar por encima de `published_sequence` verificado.

### 14.8 El contrato de ACK (V1)

Un solo contrato, para todos los volúmenes. El atributo `durability` por volumen y el par
`remote`/`local` se retiraron con **ADR-0026**: un modo que nadie selecciona es un segundo
contrato que mantener correcto gratis.

```text
FLUSH/FUA → fdatasync local → ACK        (sin esperar a S3, sin depender del lease)
```

Reglas:

1. **El ACK es local.** `fdatasync` es una garantía real frente a la caída del proceso, del
   Agent y de QEMU. La pérdida del host queda fuera, y eso es el RPO de §2.
2. **S3 recibe el volumen al parar**, no en cada FLUSH: una subida por sesión, más una copia
   por snapshot (§19). Es la única durabilidad que sale del host, y es la que un arranque
   posterior o un clon leen.
3. **Dos incarnaciones no pueden subir las dos.** Es lo único que el fencing tiene que
   garantizar en V1 (§12): un compare-and-set sobre un objeto en el momento de parar. Dos
   hosts subiendo es una pérdida de actualización silenciosa, y es la propiedad que no se
   relaja al simplificar.
4. **Los snapshots son completos en S3** (§19): `fsync` y una copia congelada en un número de
   secuencia. Es el mecanismo para obtener un punto portable bajo demanda.
5. Los límites de unflushed (§5.7) y el backpressure aplican igual.
6. **El guest no puede detectar nada de esto.** El contrato es con el operador.

---

## 15. Cifrado en reposo (nuevo)

### 15.1 Modelo de claves

```text
KEK (por instalación o por tenant)  →  vive en KMS (AWS KMS / Vault;
                                       mínimo aceptable on-prem: KEK por host
                                       en archivo con permisos estrictos)
DEK (por volumen, AES-256)          →  generada al crear el volumen;
                                       almacenada SOLO envuelta (dek_wrapped en PG
                                       y en el descriptor del volumen en S3)
```

- El Agent hace unwrap de la DEK únicamente en attach/recovery (una llamada al KMS, fuera del data path) y la mantiene solo en memoria.
- `KeyID` en cada record/objeto permite rotación de DEK sin re-cifrar histórico (la rotación aplica a datos nuevos; el histórico se re-cifra, si se desea, vía compactación).

### 15.2 Cifrado de datos

- AES-256-GCM por payload de record y por chunk de segmento/checkpoint.
- Nonce **derivado determinísticamente** de `(volume_id, epoch, sequence)` (o `(segment_id, chunk_index)` para segmentos): sin estado extra, sin riesgo de reuso dentro de un epoch por la monotonicidad de sequence.
- Overhead de CPU con AES-NI: GB/s por core; irrelevante frente a la latencia de red.

### 15.3 Consecuencias operativas

- **Crypto-shredding**: borrar un volumen = destruir su DEK envuelta. Los objetos remanentes son ruido indescifrable; el GC deja de ser urgente para compliance.
- El server-side encryption del bucket puede sumarse, pero no sustituye al cifrado en el Agent (protege del acceso al bucket, no del operador del object store).
- Los manifests/descriptors no contienen datos del guest: pueden quedar en claro para permitir `rebuild-metadata`.

Por qué día 1: re-cifrar petabytes de objetos inmutables después es un proyecto de migración; reservar los campos (`KeyID`, `AuthTag`) ahora es gratis.

---

## 16. Máquina de estados del Volume Agent

Estados por volumen (cambios v5 en negrita):

```text
DETACHED → ATTACHING → ACTIVE
ACTIVE ⇄ SNAPSHOTTING (en background, sin quiesce; §19)
ACTIVE → SELF_FENCED   (lease vencido según reloj monotónico local)  ← nuevo
ACTIVE → FENCED         (notificación de epoch inválido)
FENCED / SELF_FENCED → (espera de instrucciones)
RECOVERY_REQUIRED → RECOVERING → ACTIVE (nuevo epoch)
fallo en RECOVERING → FAILED | RECOVERY_REQUIRED (requiere intervención)
```

### Transiciones críticas

**ATTACHING → ACTIVE**

1. Validar epoch + obtener lease del Control Plane.
2. Unwrap de la DEK (KMS).
3. Cargar último checkpoint (local o S3).
4. Reproducir WAL local + remoto hasta el punto durable **determinado por S3** (§22.1).
5. Reconstruir active map.
6. Abrir socket vhost-user (con inflight region).
7. Crear/preparar efímero.
8. Publicar ACTIVE.

**ACTIVE → SELF_FENCED** (nuevo)

- `monotonic_now() - t0 >= lease_ttl` sin renovación.
- Dejar de ACKear FLUSH/FUA inmediatamente (regla 12.2). Fallar requests durables pendientes.
- No publicar batches nuevos, checkpoints ni manifests.

**ACTIVE → FENCED**

- Igual que v4: notificación de epoch inválido u otro writer con lease.

**RECOVERING**

1. Confirmar con el CP el nuevo epoch y el lease.
2. Determinar el punto durable listando S3 (summary + prefijo contiguo, §22.1).
3. Descargar checkpoint + WAL objects necesarios; verificar continuidad y checksums (CRC + GCM).
4. Reproducir; escribir `recovery-point.json` del nuevo epoch (§12.5).
5. Crear efímero vacío.
6. Solo entonces ACTIVE.

### Reconexión vhost-user (proceso del Agent, no por volumen)

Crash/deploy del Agent: las requests en vuelo se recuperan de la inflight shared memory; QEMU reconecta al socket; pausa de I/O de segundos sin reinicio de VM. Todo deploy del Agent es un rolling restart transparente. (Implementación de referencia: SPDK vhost. QEMU pineado en 11.0.2; el protocolo vhost-user incluye `VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD` — el front-end entrega el buffer inflight compartido de vuelta al backend tras crash/restart.)

---

## 17. Semántica de I/O

### WRITE normal

```text
Guest WRITE (extent real)
→ append al WAL local (cifrado)
→ actualizar interval map / active map
→ ACK
```

Puede perderse si el host se pierde antes de FLUSH/FUA/snapshot (write-back anunciado).

### WRITE + FUA / FLUSH

Ver §14.4 (incluye la verificación de lease previa al ACK).

### DISCARD / WRITE_ZEROES

```text
Guest DISCARD
→ record sin payload en WAL local
→ actualizar mapas (rango ausente / ceros)
→ ACK
```

> Todo write cubierto por FLUSH/FUA sobrevive a la pérdida completa del host, **incluida la partición del writer** (garantizado por §12).

---

## 18. Idempotencia

- PUTs de datos: clave determinística + `If-None-Match: *` + retry/HEAD/checksum (§14.5).
- Operaciones administrativas: `request_id` UUID único en **todas** las mutaciones (attach, detach, snapshot, clone, resize, drain, recovery), materializado en la tabla `operations`.
- Publicaciones de baja frecuencia (checkpoint, manifest): CAS contra el objeto de epoch (§12.4).
- Mismo rango con hash distinto → corrupción o divergencia → fallo duro.

---

## 19. Snapshot sin pausa (rediseñado)

El estado es un log totalmente ordenado por `sequence`: un snapshot es un **número**, no un evento que drena colas.

```text
1. Capturar atómicamente N = local_sequence (bajo el lock del volumen; µs).
2. Seguir aceptando writes normalmente (sequences > N).
3. En background (clase flush para los PUTs que le den durabilidad a N):
   a. Cerrar/subir batches hasta N (pipeline paralelo).
   b. Sellar una vista CoW del active map a sequence N.
   c. Calcular root digest.
   d. (checkpoint_on_snapshot) objectizar segmentos, clase background.
   e. Publicar manifest en S3 (último objeto de datos; validado por CAS de epoch).
4. Registrar PUBLISHED en PostgreSQL (request_id).
5. Crear active child.
```

- Pausa percibida por el guest: ~0. Estados: `CREATING → PUBLISHED | FAILED → DELETING`.
- El default es **crash-consistent** (equivalente a EBS/RBD). Consistencia a nivel aplicación (freeze del filesystem vía qemu-guest-agent) es un **flag opcional**, no el default.
- Métrica nueva obligatoria: `snapshot_publish_duration_seconds` (y `snapshot_pause_duration_seconds`, que debe permanecer ~0).

---

## 20. Clone, localidad y cadenas

Orden de placement:

```text
1. source_host con capacidad (según contabilidad de hosts, §8)
2. host con snapshot cacheado (incluido el standby tibio)
3. cualquier host con capacidad
```

Same-host: sin descarga; reutiliza EROFS, checkpoint y WAL cacheado; nuevo active child + efímero.

Cross-host: descarga checkpoint + WAL posteriores; materializa completo; arranca. (Standby tibio y, después, lazy loading acortan esto; §22.3–22.4.)

### 20.1 Aplanado de cadenas (nuevo)

Cada clone-de-clone encadena checkpoints + WAL; el replay y el read path degradan linealmente con la profundidad. Regla:

```text
si chain_depth > max_chain_depth (5)  o  replay_estimado > umbral:
    programar FLATTEN (operación reconciliada, clase background):
    materializar un checkpoint "full" autocontenido para ese linaje
```

Equivalente al `flatten` de RBD / compactación de niveles de un LSM. Sin esto, el sistema funciona en la demo y degrada en silencio durante meses. Métricas: `chain_depth{volume}`, `flatten_operations_total`.

---

## 21. Objectization, compactación y GC

### 21.1 Objectization

Valores iniciales:

```text
segment_target_size:     128 MiB
checkpoint_interval:     256 MiB de WAL o 2 minutos
checkpoint_on_snapshot:  true
```

Orden estricto (sin cambios de fondo vs v4, con CAS agregado):

1. Crear segmentos (aplicando DISCARDs).
2. Subir segmentos (clase background).
3. Verificarlos (HEAD + checksum).
4. Publicar checkpoint (validando epoch por CAS).
5. Publicar manifest.
6. Actualizar PostgreSQL.
7. Marcar como elegible el WAL local con `sequences <= published_sequence`.

Nunca truncar WAL local antes de que el checkpoint correspondiente esté verificado en S3.

### 21.2 Compactación de WAL objects (nuevo)

Los workloads fsync-pesados generan inevitablemente objetos chicos (un PUT por flush). Un compactador de clase background:

- Fusiona WAL objects chicos ya durables en objetos de 64–128 MiB (mismo contenido lógico, claves nuevas `walc/<vol>/<epoch>/...`).
- Actualiza el summary object; los originales quedan huérfanos y los recoge el GC.
- Nunca toca objetos referenciados por un manifest sin reescribir el manifest de forma transaccional (CAS).

Beneficios: costo de requests y de LIST, cardinalidad de objetos, y velocidad de replay en recovery. Patrón tomado de los sistemas log-sobre-S3 (WarpStream / LSMs). Métricas: `compaction_bytes_total`, `compaction_objects_merged_total`.

### 21.3 GC seguro: mark-and-sweep sin borrado directo (nuevo)

El incidente más probable de pérdida de datos en este sistema no es un crash: es **un bug del GC que borra objetos vivos**. Diseño por etapas (v5.1):

**Día 1 (todo entorno con datos que importen)**: Versioning + lifecycle. **El GC no tiene permisos de borrado permanente desde el día 1** — sobre un bucket versionado, un delete marker es reversible; esta protección no requiere Object Lock.

**Gate de producción**: Object Lock (modo governance) se activa **antes del primer dato de producción real** (no "cuando el GC madure" a secas). Es la capa anti-ransomware/anti-operador; en staging on-prem puede diferirse para reducir fricción con los backends.

Mecánica:

1. **Mark**: el GC (en el CP) recorre descriptors + manifests + recovery-points y computa el conjunto alcanzable. Todo lo no alcanzable y más viejo que el grace period (24–48 h) se marca (tag o delete marker versionado).
2. **Sweep**: lo ejecuta el **lifecycle del bucket** expirando versiones no-actuales tras el grace period. El GC nunca ejecuta `DeleteObject` permanente ni bypass de Object Lock.

Un GC que no puede borrar permanentemente no puede causar el peor incidente. Costo: storage extra durante el grace period — el seguro más barato disponible. Métricas: `orphan_objects_total`, `gc_marked_bytes_total`, `gc_reclaimed_bytes_total`.

---

## 22. Recovery, standby y reconstrucción

### 22.1 Determinación del punto durable (autoridad: S3)

```text
1. GET volumes/<vol>/epoch → epoch fenced más alto E (fallback: LIST de prefijos de epoch).
2. GET wal/<vol>/<E>/summary.json (si existe) → último estado conocido + rangos.
3. LIST desde el punto del summary; verificar prefijo CONTIGUO de sequences.
4. punto_durable = fin del prefijo contiguo. Objetos más allá de un gap se ignoran
   (y serán GC'd como huérfanos).
```

**Summary object** (evita LISTs masivos con muchos objetos): el Agent escribe periódicamente (ej. cada 30 s de actividad o cada checkpoint) un objeto pequeño `wal/<vol>/<epoch>/summary.json` con el último sequence durable conocido y la lista de rangos/objetos. Recovery = 2 GETs + 1 LIST corto. (Modelo tomado de Litestream: generations = epochs, restore = replay del prefijo contiguo listado desde S3.)

### 22.2 Mismo host

1. Reiniciar Agent (las VMs no se reinician: reconexión vhost-user, §16).
2. Validar epoch + lease.
3. Cargar checkpoint; escanear WAL local; completar contra WAL remoto.
4. Reconstruir active map.
5. Reabrir sockets; recuperar inflight.

### 22.3 Pérdida del host — con standby tibio (nuevo)

Para volúmenes marcados (`standby_host_id`), el host secundario mantiene en background (con presupuesto):

- El último checkpoint descargado.
- Opcionalmente, WAL objects recientes.

Failover: `FENCING_WAIT` (12 s) → replay del delta desde el checkpoint → boot. **RTO en minutos** en lugar de horas. Es un cron con métricas, no un sistema de replicación: `standby_checkpoint_lag_bytes` con alerta si el standby se atrasa.

Sin standby (frío): materialización completa; el RTO se publica **por GiB medido** en el runbook, para que el operador sepa cuánto tarda antes de apretar el botón.

### 22.4 Lazy loading (diseñado ahora, implementado después)

Modelo EBS-restore / overlaybd / Nydus: boot inmediato, bloques on-demand desde S3 (primera lectura lenta) + hidratación en background con presupuesto. El formato de checkpoint y el active map de v5 ya son compatibles (bitmap de presencia por segmento); no requiere cambios de formato cuando se implemente.

### 22.5 `rebuild-metadata`: reconstrucción total de PostgreSQL desde S3 (nuevo)

El layout en S3 es autodescriptivo para que la pérdida total de PG sea recuperable en horas:

```text
volumes/<vol>/descriptor.json    # tamaño, kek_id, dek_wrapped, epoch history,
                                 # linaje de snapshots, chain_depth
volumes/<vol>/epoch              # epoch vigente (objeto CAS)
snapshots: manifests con parentesco completo (parent_snapshot_id, root, rangos)
wal/<vol>/<epoch>/recovery-point.json y summary.json
```

Herramienta `rebuild-metadata`: escanea los buckets, valida consistencia y repuebla PG. Complementos: PITR de PostgreSQL (WAL-G/pgBackRest hacia el mismo object store) con restore **ensayado** trimestralmente.

### 22.6 Runbook mínimo de recovery

Documentos operativos exigidos por esta arquitectura desde el día 1: failover de writer (con los tiempos de `FENCING_WAIT`), restore de PG (PITR y rebuild), pérdida de nodo del object store, break-glass del CP, y RTO medido por escenario.

---

## 23. Edge cases

### S3 caído

- WRITEs normales se acumulan hasta los límites de unflushed.
- FLUSH/FUA no completan (error o bloqueo según política).
- Snapshots no publican.
- Backpressure estricto antes de llenar NVMe.
- El circuit breaker del cliente S3 (§24) evita tormentas de retries.

### PostgreSQL caído

- VMs activas continúan **mientras sus leases sigan renovándose**… y como las renovaciones pasan por el CP→PG, con PG caído los leases dejan de renovarse: a los `lease_ttl` los Agents entran en `SELF_FENCED` para durabilidad (dejan de ACKear FLUSH) aunque sigan sirviendo reads y aceptando writes no durables según política.
- **Excepción v5.1**: los volúmenes en modo `local` (§14.8) no se ven afectados en FLUSH — su ACK no depende del lease. La degradación aplica solo a volúmenes `remote`.
- **Decisión explícita**: PG caído degrada durabilidad confirmable de los volúmenes `remote` en ~`lease_ttl`. Es el precio de un fencing simple y correcto sin consenso propio. Mitigación: PG con standby + PITR; el modo degradado y sus tiempos van en el runbook y en la documentación al usuario.
- No hay attach, clone, snapshot, cambio de epoch ni recovery mientras dure.

### PUT exitoso con respuesta perdida

Retry idempotente + HEAD + checksum.

### WAL remoto subido, manifest no publicado

Huérfanos → GC (mark por inalcanzabilidad + lifecycle).

### Manifest publicado, PostgreSQL no actualizado

El reconciler completa la operación usando `request_id` (o `rebuild-metadata` en el caso extremo).

### Old writer vuelve

- No pudo ACKear nada tras vencer su lease (12.2).
- Sus PUTs tardíos quedan fuera del recovery-point y son GC'd (12.5).
- Sus publicaciones de checkpoint/manifest fallan por CAS de epoch y por validación en PG.

### NVMe lleno

1. Detener prefetch/objectization no crítica (clase background ya limitada).
2. Priorizar uploads de batches pendientes.
3. Evictar caché limpia.
4. Backpressure (rechazar WRITEs).
5. Fallar explícito antes de comprometer la capacidad de recovery.

### Reloj de pared con deriva excesiva

- chrony reporta offset > 500 ms → alerta; > `max_clock_skew` → el host se marca no elegible para promociones y se investiga. La seguridad del writer viejo no depende del reloj de pared (usa monotónico), pero la espera de promoción sí asume la cota.

### Crash del Agent con requests en vuelo

Inflight shmfd: al reiniciar, el Agent recupera y completa/reintenta las requests sin duplicar ni perder (verificado por DST).

---

## 24. Cliente S3 como subsistema (nuevo)

Aquí vive la latencia de cola del sistema. Requisitos:

- **Hedged GETs**: si un GET (recovery, read-miss, hidratación) no respondió al p95 histórico, lanzar un segundo en paralelo y tomar el primero. Recorta el p99 dramáticamente ("The Tail at Scale").
- **Retry budget global + circuit breaker por backend**: presupuesto de retries por segundo con backoff exponencial + jitter; nunca retries infinitos por request. Bajo throttling, degradar coordinadamente en lugar de amplificar.
- **Límites de ancho de banda por clase de I/O** (§11): flush tiene prioridad absoluta; background (hidratación, compactación, materialización) con presupuesto. Un recovery grande no puede destrozar la latencia de FLUSH de las demás VMs del host.
- **Pool de conexiones** dimensionado; keep-alive; reutilización TLS.
- **Prefijos**: `wal/<volume_id>/...` ya distribuye bien para el sharding por prefijo de S3; verificar ausencia de hot-spotting en MinIO/RustFS con este layout.

Métricas: `s3_request_latency_seconds{op,class,hedged}`, `s3_retry_budget_exhausted_total`, `s3_circuit_open_seconds`.

---

## 25. Verificación: DST, fault injection y property tests

### 25.1 Deterministic Simulation Testing (focalizado, estructural desde el primer commit)

Para un sistema cuyo contrato es "cero pérdida de writes con FLUSH", la diferencia entre *creer* el invariante y *demostrarlo* es DST (técnica de FoundationDB, TigerBeetle, WarpStream). Alcance v5.1 — lo estructural no se relaja, el volumen sí:

**Obligatorio desde el commit 1** (imposible de retrofitear):

- **Interfaces simulables** para reloj (monotónico y de pared), red, disco y S3. Ninguna llamada directa a `time.Now()`, sockets o syscalls de disco fuera de esas interfaces.
- **Checkers de invariantes en el harness** aunque haya pocos escenarios: prefijo durable contiguo, watermarks ordenados, single-writer efectivo, ningún ACK durable fuera del prefijo recuperable (parametrizado por modo de durabilidad, §14.8), ningún objeto vivo marcado por GC.
- Property tests del WAL (§25.2).
- **Set mínimo de escenarios como gate de CI en cada PR**: fencing con partición del writer (con acceso a S3 intacto), PUT exitoso con respuesta perdida, crash alrededor de append/fdatasync/PUT/ACK, deriva de reloj inyectada.

**Incremental** (crece con el sistema, no bloquea el arranque): la corrida nocturna de alto volumen de semillas con fallas aleatorias en cada punto de decisión. Cada bug encontrado se reproduce exactamente con su semilla y se congela como escenario de regresión. El conteo de semillas crece gratis una vez que el harness y los checkers existen; los checkers retrofiteados, no.

### 25.2 Property tests del WAL

Para el par serialize/replay: cualquier secuencia de records (writes/discards/zeroes con extents arbitrarios) → replay produce el estado esperado, con truncamientos arbitrarios en cada byte del archivo y corrupciones de bit aleatorias (detectadas por CRC/GCM, nunca aplicadas en silencio).

### 25.3 Fault injection sobre hardware real (complemento, no sustituto)

La lista de v4 se mantiene íntegra (kills alrededor de append/fdatasync/PUT/ACK, duplicados, throttling, PG caído, epoch con respuesta perdida, disco lleno, corrupciones) y se agrega:

- Kill del Agent con requests en vuelo (verificar recuperación por inflight shmfd).
- Partición del writer con acceso a S3 intacto (verificar SELF_FENCED antes del ACK).
- Deriva de reloj inyectada hasta y más allá de `max_clock_skew`.
- Compactación concurrente con recovery.
- GC ejecutando mark durante publicación de manifest.
- Backend sin soporte CAS (degradación correcta a lease-only).

### 25.4 Suite de conformidad del backend

Ver §6.1; bloqueante para cada versión de MinIO/RustFS/S3 que se habilite.

---

## 26. Observabilidad y trazabilidad

### 26.1 Tracing distribuido (nuevo)

`request_id`/`operation_id` se propagan como trace context (OpenTelemetry) por CP → Agent → cliente S3 → KMS. Logging estructurado con esos campos en todos los componentes. El día que un FLUSH tarde 4 s, el MTTR es "abrir el trace", no "grep en tres hosts". Decisión de librería al inicio; costo marginal.

### 26.2 Métricas

**WAL local**: `wal_append_latency_seconds`, `wal_fdatasync_latency_seconds`, `wal_unflushed_bytes`, `wal_oldest_unflushed_age_seconds`, `wal_{local,durable,published}_sequence`.

**WAL remoto**: `wal_batch_size_bytes`, `wal_batch_age_seconds`, `wal_put_latency_seconds`, `wal_put_retries_total`, `wal_small_batch_ratio`, `wal_durable_gap` (bytes) y `wal_durable_gap_seconds` (RPO efectivo por volumen, crítico en modo `local`), `wal_objects_total{volume}`.

**Fencing y leases** (nuevo): `lease_remaining_seconds`, `lease_renewal_failures_total`, `self_fenced_total`, `fencing_wait_duration_seconds`, `clock_offset_seconds` (chrony).

**Snapshots** (nuevo): `snapshot_publish_duration_seconds`, `snapshot_pause_duration_seconds` (~0 esperado).

**Objectization/compactación/GC**: `checkpoint_duration_seconds`, `objectization_pending_bytes`, `compaction_bytes_total`, `orphan_objects_total`, `gc_reclaimed_bytes_total`, `chain_depth`, `discarded_bytes_total`.

**Recovery/standby**: `recovery_duration_seconds`, `standby_checkpoint_lag_bytes`, `bytes_downloaded_before_boot`, `s3_errors_total{type}`.

**Agent** (nuevo): `agent_memory_bytes{component}`, `active_map_bytes`, `io_class_bytes_total{class}`, `io_class_throttled_seconds`, `vhost_reconnects_total`, `inflight_recovered_total`.

**Fleet** (nuevo): `host_nvme_committed_ratio`, `clone_{same,cross}_host_total`.

**Cliente S3**: ver §24.

### 26.3 Alertas mínimas

- `wal_unflushed_bytes > 80%` del límite; `wal_oldest_unflushed_age > 20 s`.
- `wal_put_latency p99 > 500 ms`.
- `lease_renewal_failures_total` creciendo; cualquier `self_fenced_total`.
- `clock_offset_seconds > 0.5`.
- `standby_checkpoint_lag_bytes` sobre umbral.
- `host_nvme_committed_ratio` sobre la política.
- Certificados mTLS a < 30 días.
- Cualquier error de checksum, GCM o divergencia (severidad máxima).
- `snapshot_pause_duration_seconds > 0` sostenido (regresión del diseño sin pausa).

---

## 27. Versionado de formatos y fleet mixto (nuevo)

Sin esta política, el primer cambio de formato con el fleet a medias actualizado produce un volumen ilegible.

1. **Read-old ilimitado**: un Agent vN+1 lee todo formato de su misma major (WAL, segmentos, checkpoints, manifests, descriptors).
2. **Write-new gated**: los formatos nuevos se activan por feature flag **después** de que todo el fleet puede leerlos (dos fases: desplegar lectores → habilitar escritores).
3. El CP registra `max_format_version` por host (tabla `hosts`) y **rechaza** operaciones incompatibles (ej. recuperar en un host viejo un volumen escrito con formato nuevo; placement de clones sobre hosts que no leen el formato del snapshot).
4. Misma disciplina para el API gRPC del CP (compatibilidad hacia atrás dentro de la major; deprecaciones anunciadas).
5. Los headers ya llevan `Version` + magic distintos por tipo; los campos `Reserved` existen para extensiones sin bump de versión.

---

## 28. Operación de fleet (nuevo)

### 28.1 Cordon / Drain

- `cordon <host>`: no colocar nada nuevo (estado en tabla `hosts`).
- `drain <host>`: operación reconciliada de larga duración; mueve volúmenes fuera (snapshot + restore cross-host en el MVP, priorizando destinos con caché/standby), con progreso visible (`operations`) y cancelable. Prerequisito de todo mantenimiento de kernel/NVMe/decomiso.

### 28.2 Capacidad y placement

Heartbeat del Agent reporta NVMe total/usado/comprometido. El CP aplica la política de sobresuscripción declarada al decidir placement, y la alerta `host_nvme_committed_ratio` avisa antes del incidente.

### 28.3 Deploys

- Agent: rolling restart transparente (reconexión vhost-user). Regla: nunca desplegar un writer de formato nuevo antes de completar la fase de lectores (§27).
- CP: standby toma liderazgo por término; las operaciones en curso re-convergen por reconciliación.
- Backend de objetos: rolling según su propio procedimiento; la suite de conformidad (§6.1) corre contra la versión nueva antes.

### 28.4 Runbooks exigidos

Failover de writer, restore de PG (PITR + rebuild-metadata), pérdida de nodo del object store, drain, break-glass del CP, rotación de KEK, respuesta a alerta de divergencia/checksum. Cada uno con tiempos medidos, no estimados.

---

## 29. Debilidades residuales y mitigaciones explícitas

Actualizado: se eliminan las resueltas por diseño en v5 y se agregan las nuevas que v5 introduce.

### 1. Latencia de FLUSH/FUA ligada al object store (sigue siendo la más importante)

**Problema**: workloads con fsync frecuente pagan red + PUT por commit. Es inherente al modelo sin réplica host-to-host.

**Mitigaciones**: pipeline de PUTs paralelos en FLUSH; backend en la misma red/región; documentación explícita del write-back al usuario (§2); p99 medido desde el día 1 con inyección de latencia; compactación para que el costo de los objetos chicos no se acumule. Futuro: flush selectivo para FUA; modo "durable writes" opcional por volumen.

### 2. Disponibilidad de durabilidad acoplada a PostgreSQL vía leases (nueva, introducida por el fix de fencing)

**Problema**: con PG caído, los leases no se renuevan y en ~`lease_ttl` los Agents dejan de poder ACKear FLUSH (SELF_FENCED para durabilidad).

**Mitigación**: es un trade-off aceptado a cambio de un fencing correcto sin consenso propio, y **en v5.1 aplica solo a volúmenes `remote`** — los volúmenes `local` (§14.8) no dependen del lease para ACKear FLUSH. PG con standby + PITR; el modo degradado, sus tiempos y su alarma están especificados (§23). Alternativa futura si duele: renovación de lease vía objeto CAS en S3 como camino secundario.

### 3. Dependencia de cotas de deriva de reloj para la promoción

**Problema**: `FENCING_WAIT` asume `max_clock_skew = 2 s`.

**Mitigación**: la seguridad del writer viejo usa solo reloj monotónico (no depende del supuesto); chrony monitoreado con alerta a 500 ms; hosts con deriva excesiva quedan inelegibles para promoción; DST inyecta deriva más allá de la cota para verificar que el único efecto es esperar de más, nunca perder writes.

### 4. Materialización completa en cross-host frío

**Mitigación**: standby tibio para volúmenes que importan (RTO en minutos); RTO frío publicado por GiB; lazy loading diseñado y compatible con el formato actual, se implementa post-MVP.

### 5. Dependencia de semántica S3-compatible (ahora mayor: CAS, versioning, Object Lock)

**Mitigación**: suite de conformidad bloqueante por versión de backend (§6.1); degradación especificada si falta CAS (lease-only sigue siendo seguro); requisito de durabilidad mínima on-prem (§6.1) elimina el caso single-node.

### 6. PostgreSQL como punto único de control

**Mitigación**: VMs siguen sirviendo I/O no durable; reconciliación hace trivial el catch-up al volver; PITR + `rebuild-metadata` cubren desde el blip hasta la pérdida total; término verificado elimina el split-brain del CP.

### 7. Complejidad agregada por v5 (nueva, honesta)

**Problema**: cifrado, compactación, DST, clases de I/O y reconciliación son más piezas que en v4; el riesgo pasa de "diseño incorrecto" a "superficie de implementación".

**Mitigación**: el roadmap secuencia las piezas para que cada fase sea verificable por DST antes de la siguiente; las piezas de "día 1" son mayormente interfaces y campos reservados (baratos ahora, imposibles después); compactación, standby y aplanado son opcionales para el primer despliegue y se activan por flag.

### 8. Orden de objectization / truncado

Sin cambios: orden estricto + DST/fault injection en cada paso + nunca truncar por encima de `published_sequence` verificado + GC sin borrado directo como red final.

---

## 30. Roadmap

Reordenado: DST e interfaces simulables van primero (estructurales); la reconexión vhost-user sube (era el punto 10 en v4 y es lo que hace operables los deploys); el fencing completo llega junto con los epochs.

1. **Esqueleto con interfaces simulables** (reloj/red/disco/S3) + harness DST mínimo + tracing/logging estructurado. *Nada de `time.Now()` directo desde el primer commit.*
2. Layout local + tres dispositivos + OverlayFS.
3. vhost-user-blk con backend raw + **reconexión + inflight shmfd**.
4. CoW (segmentos 64 KiB) + WAL local con **extents reales** + formato v2 con campos de cifrado + property tests del WAL.
5. **Cifrado por volumen** (DEK/KEK, KMS de desarrollo) + DISCARD/WRITE_ZEROES.
6. WAL remoto: batching bajo demanda + idempotencia de PUT + summary objects.
7. PostgreSQL + Control Plane con **término verificado** + modelo de reconciliación + **leases y protocolo de fencing completo** (§12) bajo DST con particiones y deriva de reloj.
8. Recovery con **S3 como autoridad** + recovery-point + `rebuild-metadata` básico.
9. Snapshots sin pausa + clones same-host + resize (grow).
10. Objectization + checkpoints + GC mark-and-sweep (buckets con versioning + Object Lock desde el primer entorno de staging).
11. Cross-host por materialización completa + cordon/drain + contabilidad de capacidad.
12. Standby tibio + compactación de WAL objects + aplanado de cadenas.
13. Endurecimiento: fault injection sobre hardware real, suite de conformidad de backends, runbooks con tiempos medidos.
14. Post-MVP: lazy loading (overlaybd-style), multi-queue, io_uring, flush selectivo FUA, QoS entre tenants, renovación de lease vía S3.

---

## 31. Criterios de éxito del MVP

1. Boot con los tres dispositivos.
2. Docker/containerd solo en efímero.
3. FLUSH exitoso en modo `remote` sobrevive a pérdida del host **y a la partición del writer** (demostrado por el harness DST con checkers de invariantes; el volumen de semillas crece incrementalmente).
3b. En modo `local`: RPO medido ≤ `max_unflushed_age` en el test de pérdida de host, y snapshots siempre completos en S3.
4. Ningún ACK de durabilidad emitido con lease vencido (invariante DST).
5. Snapshot portable, crash-consistent, con `snapshot_pause_duration ≈ 0`.
6. Clone same-host sin transferencia significativa.
7. Clone cross-host desde S3; RTO frío medido y publicado; RTO con standby tibio en minutos.
8. Stale writer incapaz de confirmar durabilidad tras `lease_ttl` (verificado con partición + acceso a S3 intacto).
9. PUT idempotente (incluyendo respuesta perdida).
10. Recovery cuyo punto durable se determina desde S3 y coincide con todo lo ACKeado.
11. Crash/deploy del Agent sin reinicio de VMs (inflight recuperado, cero I/O perdido o duplicado).
12. Todo dato de VM fuera del host cifrado; borrado de volumen = crypto-shred.
13. Backpressure estricto antes de llenar NVMe.
14. WAL nunca eliminado antes de durabilidad remota verificada; GC incapaz de borrado permanente directo.
15. DISCARD reduce `wal_objects`/segmentos: el storage converge al working set en el test de churn.
16. Resize (grow) online end-to-end.
17. `rebuild-metadata` reconstruye PG desde S3 en un entorno de prueba.
18. Fleet mixto vN/vN+1 opera sin volúmenes ilegibles (test de upgrade en CI).
19. Métricas y trazas mínimas visibles desde el día 1; runbooks con tiempos medidos.

---

## 32. Conclusión

```text
PostgreSQL
    └── leases, epochs, términos del CP, metadata, reconciliación, request_id
        (caché/control; reconstruible desde S3)

Control Plane single-active (término verificado por transacción)
    └── attach, snapshot, clone, resize, drain, promoción con FENCING_WAIT,
        reconciler, GC (solo mark), capacidad y placement

Volume Agent por host
    ├── vhost-user-blk (1 queue) + reconexión + inflight shmfd
    ├── CoW: segmentos 64 KiB; WAL por extents reales
    ├── Cifrado AES-256-GCM por volumen (DEK/KEK)
    ├── WAL local append-only + batcher bajo demanda (8/16 MiB, 20 s)
    ├── Lease manager (reloj monotónico; ACK durable solo con lease vigente)
    ├── Clases de I/O: foreground / flush / background con presupuestos
    ├── Cliente S3: hedged GETs, retry budget, límites por clase
    ├── Límites duros de unflushed + presupuesto de memoria
    └── Objectizer + compactador + standby hidrator (background)

S3 (backend con durabilidad exigida; versioning + Object Lock en prod)
    ├── EROFS · WAL objects · segmentos · checkpoints
    ├── manifests · descriptors · summary · recovery-points · objeto epoch (CAS)
    └── AUTORIDAD del punto durable en recovery
```

```text
WRITE normal   → WAL local (extent real, cifrado) → ACK
DISCARD        → record sin payload → ACK
FLUSH / FUA    → fdatasync local → PUTs paralelos verificados
               → lease vigente (reloj monotónico) → durable_sequence → ACK
SNAPSHOT       → capturar N → publicar en background → pausa ≈ 0
```

No existe replicación host-to-host. La durabilidad vive en S3 y su límite es la durabilidad del backend, que ahora es un requisito explícito, no un supuesto. El fencing ya no tiene ventana: un writer particionado no puede confirmar durabilidad pasado su lease, y el nuevo writer solo arranca después de esa garantía. La verdad en recovery se lee de S3, y PostgreSQL entero es reconstruible. Todo lo que sale del host va cifrado. El sistema está diseñado para el día 2: deploys sin reiniciar VMs, GC incapaz de causar el peor incidente, drains reconciliados, RTOs medidos, y un contrato de durabilidad demostrado millones de veces por noche en simulación determinística — no solo creído.

Esta v5 es la base recomendada para empezar a codificar, comenzando por la fase 1 del roadmap: las interfaces simulables no son opcionales ni postergables.

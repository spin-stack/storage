# Arquitectura MVP de volúmenes remotos para máquinas virtuales (v5)

## EROFS, CoW por bloques, vhost-user-blk, PostgreSQL y WAL durable en S3/RustFS — con fencing formal, cifrado y diseño operativo

- **Fencing formal** con leases sobre reloj monotónico, espera de promoción y CAS del objeto epoch en S3. Cierra la ventana de pérdida de writes confirmados por un stale writer (era la debilidad de correctness más grave de v4).
- **S3 es la autoridad de recovery**; PostgreSQL pasa a ser caché/control. Nueva regla formal del punto durable + summary objects para acelerar recovery.
- **Término (`term`) verificado en cada transacción del Control Plane**; el advisory lock queda solo como optimización.
- **Cifrado en reposo por volumen** (AES-256-GCM, DEK/KEK, crypto-shredding). Campos reservados en todos los formatos desde el día 1.
- **WAL con extents reales del guest** (offset + length). Elimina la amplificación 4→64 KiB del data path. *(La segunda mitad de este bullet decía «los 64 KiB quedan solo como granularidad de segmentos CoW»; esa granularidad **se retira** — ver §13.1 y DEV-0024.)*
- **Batching bajo demanda**: se elimina la regla de cierre por 50 ms. PUT solo ante FLUSH/FUA pendiente, tamaño objetivo o antigüedad cercana al límite. Reduce la cardinalidad de objetos en ~2-3 órdenes de magnitud para workloads sin fsync frecuente.
- **Compactación de WAL objects** en background y **GC mark-and-sweep sin permiso de borrado directo** (S3 Versioning + Object Lock + lifecycle).
- **Snapshots crash-consistent sin pausa** (captura atómica de sequence, sellado en background). Freeze del guest opcional, no default.
- **Reconexión vhost-user con inflight tracking** promovida al roadmap temprano (deploys del Agent sin reiniciar VMs).
- **Requisito mínimo de durabilidad del backend de objetos** en on-prem; single-node prohibido en producción.
- **Soporte de DISCARD/WRITE_ZEROES, ~~resize online (grow)~~ (V2, §3) y aplanado de cadenas de snapshots**.
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

- ~~**Modo de durabilidad dual por volumen** (`remote` | `local`)~~ — **retirado por ADR-0026 (2026-08-02).** Hay un solo contrato de ACK y es el que se llamaba `local`: FLUSH → `fdatasync` local → ACK, sin esperar a S3 y sin consultar el lease (§14.8). El volumen entero sube **al parar**. La columna `durability` que guardaba el modo tampoco existe: el esquema declarado la borró en vez de dejarla con un valor que nada consulta (`internal/schema/schema.sql`, líneas 96-102). Lo que sobrevive de este punto son los snapshots — siguen siendo completos en el object store (§19).
- **Lease por volumen como fase transitoria** (fleets < 50–100 hosts), con renovación **agrupada por host en una sola transacción** desde el día 1. La regla de ACK con reloj monotónico (§12.2) es idéntica en ambas fases. Migración a lease por host = colapso de esquema, no cambio de protocolo.
- **Object Lock diferido**: versioning + lifecycle desde el día 1; el GC sin permisos de borrado permanente desde el día 1 (delete markers reversibles); Object Lock governance como gate antes del primer dato de producción real.
- **DST focalizado**: interfaces simulables + property tests del WAL + checkers de invariantes en el harness desde el commit 1; el set obligatorio de escenarios (fencing, partición, PUT perdido) verde en cada PR. El volumen de semillas crece incrementalmente.
- QEMU pineado en 11.0.2; apalancamiento en librerías maduras (roaring bitmaps, cliente S3 con pooling, OpenTelemetry).

---

## 1. Resumen ejecutivo

El MVP expone tres dispositivos a cada VM (**el layout es del consumidor, no de este
módulo — ver el banner de §9**):

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
- Copy-on-Write sobre **los extents reales** del guest, sin grilla. *(v5.1 pedía segmentos de 64 KiB; retirado — §13.1, DEV-0024.)*
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
| Pausa de I/O por deploy/crash del Agent | **no medible todavía: el mecanismo no existe** (ver abajo) |
| Boot de un clon en el host de origen | **descarga completa igual que en cualquier otro** — ver §20 |
| Boot de un clon en otro host | descarga completa; medido por GiB, no prometido |

**El RPO de una sesión es una decisión, no una limitación pendiente de arreglar** (ADR-0026). Un host que muere a mitad de sesión pierde todo lo escrito desde que el volumen se atachó. Se documenta explícitamente al usuario, sin letra chica.

Es coherente con el caso de uso de arriba: nadie pide que un runner de CI sobreviva a la muerte de su host. La versión anterior de esta tabla pedía **RPO 0 bajo el modelo de fallas probado por DST**, y esa fila —no el caso de uso— es la que generaba la cadena de durabilidad remota completa: subida por cada FLUSH, ACK gobernado por lease, checkpoints, promoción y recuperación a mitad de sesión. Nadie la había pedido.

Un volumen que deba sobrevivir a la pérdida del host es **V2**, y vuelve con un requisito real detrás.

**La fila de la pausa por deploy/crash del Agent no está retirada: está sin construir, y
decía un número igual.** El inflight tracking es el incremento 3.3 y no se empezó;
`vhost.ProtocolFeatures` **no anuncia** `VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD`
deliberadamente, con la razón escrita al lado de la constante: anunciarlo antes de
implementar la región haría que QEMU entregue un buffer que este backend ni lee ni escribe,
y la garantía que el front-end creería entonces —las requests sobreviven un reinicio del
backend— sería falsa. Anunciar de menos degrada; anunciar de más miente. Así que hoy, un
crash o un deploy del Agent **sí** afecta a las VMs, y esta fila no tiene con qué medirse.
Es el riesgo abierto más viejo del proyecto. Las otras cinco menciones de la reconexión en
este documento —§1, §4, §10, §16, §23 y el criterio 11 de §31— describen lo mismo y hay que
leerlas contra esta fila.

---

## 3. Objetivos del MVP

1. Boot con EROFS, persistente y efímero. — **fuera de alcance de `storage` (§9).**
2. Docker/containerd únicamente sobre el disco efímero. — **fuera de alcance (§9).**
3. Read, write, flush, FUA y **discard** mediante `vhost-user-blk`.
4. Snapshots inmutables **sin pausa de I/O**.
5. Clones independientes. — **independientes para escribir, no para leer:** un clon tiene
   su propia capa y su propio ciclo de vida, y lee a través de los objetos de su linaje
   (§20). «Independiente» significa que escribirlo no toca a su padre, no que pueda
   arrancar sin él.
6. Clone en el mismo host sin descargar nuevamente el snapshot. — **no se cumple:** el
   placement lo prefiere, la descarga ocurre igual (§20).
7. Recovery desde checkpoint y WAL remoto, **con S3 como autoridad**.
8. Fencing de stale writers mediante **lease + epoch**, sin ventana de pérdida de writes confirmados.
9. Requests duplicados idempotentes.
10. Backpressure antes de llenar NVMe (límites duros de unflushed).
11. **DST + fault injection** automatizados en CI.
12. **Cifrado en reposo** de todo dato de VM fuera del host.
13. **Reconexión vhost-user**: crash o deploy del Agent no reinicia VMs.
14. ~~Resize online (grow) del volumen persistente.~~ — **V2** (banner abajo).
15. Operación local, on-premises o cloud sin Kubernetes.

> **El objetivo 14 es V2 desde 2026-08-06 (DEV-0023).** V1 no redimensiona un volumen, y
> no es un pendiente a medio terminar: lo único que existía era una fila que crecía. El
> verbo del catálogo se borró — `metadata.Store` lleva la nota donde estaba
> `ResizeVolume`, que era grow-only, term-guarded y correcto, y no tenía camino detrás.
> El desired state ya lleva `size_bytes` a cada Agent y el Agent retorna en su chequeo de
> epoch antes de leerlo (`agent.Loop.Reconcile` → `readDesiredState` → el `Reconciler`;
> no hay ningún `Loop.Apply`, y este documento lo nombró dos veces); la capacidad de un
> `blockdev.Device` la fija
> `blockdev.New` y nada la mueve después; y una capacidad nueva viajaría al guest como
> `VHOST_USER_BACKEND_CONFIG_CHANGE_MSG` por un canal de peticiones del backend que
> `internal/vhost` no ofrece (§9 lo prometía; ver ahí).
>
> Peor que incompleto: `descriptor.json` guarda el mismo tamaño y se escribe en tres
> momentos —al crear, al clonar y al aplanar (`-flatten-volume` lo reescribe)— ninguno de
> los cuales mueve el tamaño, así que un resize que llegara al catálogo y no al bucket era
> la única
> forma de que los dos discreparan sobre el tamaño de un volumen sin que nada lo notara.
> Lo que queda en su lugar es una propiedad que las dos implementaciones de
> `metadata.Store` tienen que cumplir — `VolumeGeometryIsImmutable`, en
> `internal/metadata/metadatatest`, que relee la geometría después de cada mutación — de
> modo que un resize que vuelva **como método de store y nada más** falla ahí.
>
> Las demás menciones de resize en este documento llevan «(V2, §3)». Vuelve como
> incremento cuando alguien lo pida con la mitad del Agent que nunca existió, no como
> columna.

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

> **Parte de esta tabla es V2 desde ADR-0026 (2026-08-02).** Las filas que describen la
> cadena de durabilidad remota se quedan porque siguen siendo la decisión de V2, no la de
> V1: autoridad de recovery en S3, WAL remoto y sus tamaños y reglas de cierre de batch,
> compactación, segmento objectizado, checkpoints, cross-host por materialización y
> standby tibio, y el lease como puerta del ACK. En V1 el ACK es `fdatasync` local
> (§14.8), el volumen sube al parar, y del fencing queda un compare-and-set sobre el
> manifiesto (§12). Las dos filas que afirmaban lo contrario **en presente** están
> corregidas aquí abajo; el resto se lee contra §14.8.

| Área | Decisión MVP |
|---|---|
| Control Plane | Servicio single-active con `term` verificado por transacción |
| Metadata | PostgreSQL (caché/control; **no** autoridad de recovery) |
| Autoridad de recovery | **S3** (prefijo contiguo del epoch fenced más alto) |
| Interfaz guest | virtio-blk (FLUSH + DISCARD anunciados; write-back explícito) |
| Backend QEMU | vhost-user-blk **con reconexión + inflight shmfd** |
| Imagen base | EROFS |
| Volumen durable | Un ext4 persistente (~~grow online soportado~~ — V2, §3) |
| Datos reconstruibles | Un ext4 efímero |
| Writer | Single writer |
| **Modo de durabilidad** | **Uno solo, para todos los volúmenes: FLUSH → `fdatasync` local → ACK (§14.8). El par `remote`/`local` y la columna que lo guardaba se retiraron con ADR-0026** |
| Fencing | Lease + epoch + espera de promoción (TTL + skew); regla de ACK idéntica en ambos modos de lease |
| Granularidad del lease | **Fase A (< 50–100 hosts): por volumen, renovación agrupada por host. Fase B: por host (§12.6)** |
| Cota de deriva de reloj asumida | **2 s** (NTP/chrony obligatorio y monitoreado) |
| Cifrado | **AES-256-GCM por volumen; DEK por volumen, KEK en KMS/Vault** |
| WAL local | Append-only en NVMe |
| WAL remoto | Objetos inmutables en S3 |
| Registro de WAL | **Extent real del guest (offset + length, alineado a 512 B)** |
| Granularidad CoW (segmentos) | **Retirada (DEV-0024).** No hay grilla: lo que sale del host son chunks de hasta 64 MiB (`image.MaxChunkBytes`, una **cota** y no una granularidad) direccionados por el digest de su texto plano — §13.1 |
| Tamaño objetivo de WAL object | 8 MiB |
| Tamaño máximo de WAL object | 16 MiB |
| Cierre/PUT de batch | **Bajo demanda**: FLUSH/FUA pendiente, ≥ 8 MiB, o antigüedad ≥ 20 s |
| Compactación de WAL objects | Background, objetivo 64–128 MiB por objeto compactado |
| Segmento CoW objectized | 128 MiB objetivo |
| ACK de WRITE normal | Después de append local |
| ACK de FUA/FLUSH | **`fdatasync` local, y nada más** (§14.8). Era lease vigente + PUT remoto verificado; eso es V2 |
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

El dispositivo anuncia write-back cache al guest (`VIRTIO_BLK_F_FLUSH`). Un WRITE normal se completa tras persistir localmente.

> **La tercera frase decía «la garantía frente a pérdida del host se obtiene con un FLUSH
> exitoso, un WRITE con FUA, o un snapshot publicado», y de las tres sólo la última es
> cierta en V1** — se corrige acá porque contradecía a §14.8 dentro de este mismo archivo.
> Un FLUSH es `fdatasync` local: sobrevive a la caída del proceso, del Agent y de QEMU, y
> **no** a la pérdida del host, que es el RPO de una sesión de §2. FUA no existe como
> camino: virtio-blk no tiene bit de FUA —el block layer de Linux descompone `REQ_FUA` en
> WRITE + FLUSH— y `wal.Log.Write` devuelve `ErrFUAOnWrite` si alguien pasa el flag, en vez
> de aceptarlo y no cumplirlo. Lo único que pone bytes fuera del host antes de parar es un
> **snapshot publicado** (§19), y por eso es la palanca que el runbook nombra cuando alguien
> pregunta cuánto perdería si se muere la caja.

### 5.4 Localidad no es durabilidad

La caché local acelera el arranque, pero no reemplaza S3.

> **En V1 esta frase se lee al revés, y el código es el coherente.** No hay caché local de
> datos: lo único que `internal/agent` cachea son `VolumeKeys`. Lo local no es una caché
> de S3 — es **el original**. El WAL de la sesión vive en NVMe y el object store no ve un
> byte hasta que el volumen para (§14.8), así que durante una sesión la localidad es la
> única copia que hay y S3 es la que está atrasada. El invariante sigue en pie con su
> sentido dado vuelta: *lo local no es durable*, porque perder el host pierde la sesión.
> Vuelve a leerse como está escrito cuando exista durabilidad remota continua (V2).

### 5.5 El efímero puede perderse

Nada contractualmente durable debe depender de `/dev/vdc`.

### 5.6 Watermarks ordenados

```text
published_sequence <= durable_sequence <= local_sequence
```

### 5.7 Límites de WAL local no flushed

> **Los dos valores de abajo no son configuración de V1: `wal.Limits` los tiene y ningún
> Agent los pone**, así que no existe el default de 1 GiB ni el de 30 s en ninguna
> constante. Se conservan porque describen la forma de la cota que V2 necesita; la que ata
> hoy es la tercera, `MaxLocalBytes`, y está explicada abajo.

```text
unflushed_bytes <= max_unflushed_bytes_per_volume   (default 1 GiB)
oldest_unflushed_age <= max_unflushed_age           (default 30 s)
```

Si se superan, se aplica backpressure: se rechazan nuevos WRITEs con error explícito al guest.

> **Estos dos no son la cota que ata en V1, y hasta 2026-08-07 este documento no nombraba
> la que sí (ADR-0013).** `Sync` limpia el contador de no-flusheados en cada `fsync` del
> guest, así que bajo cualquier workload que haga fsync — o sea, cualquiera al que le
> importen sus datos — `unflushed_bytes` está en cero mientras los segmentos siguen
> creciendo. Acota una ráfaga; no puede acotar una sesión, y con ADR-0026 el WAL entero de
> una sesión se queda local hasta que el volumen para.
>
> La cota que ata es **`MaxLocalBytes`: lo que este log puede ocupar del dispositivo**, y
> es una tercera regla en el mismo lugar (`wal.Limits`, comprobada en el mismo camino que
> las dos de arriba; cruzarla devuelve `wal.ErrBackpressure`, el mismo error que el guest
> entiende). No la limpia nada durante la sesión, y ésa es la forma honesta de V1: un
> volumen que gastó su parte queda en backpressure hasta que para y publica.
>
> **De dónde sale la parte**, porque un límite por volumen no es una cota sobre un
> dispositivo — N volúmenes, cada uno dentro del suyo, lo agotan entre todos y nada los
> suma. `agent.Budget` divide el dispositivo medido: `GuestRatio` (0,85) es lo que los
> guests de este host pueden llenar entre todos, `ReserveRatio` (0,05) es la resta que
> deja libre lo que la publicación al parar necesita — el `fdatasync` de segmentos ya
> escritos, que en un filesystem con delayed allocation es donde aparece el ENOSPC — y el
> resto se divide por `-max-volumes` (default `agent.DefaultMaxVolumes` = 16). La parte es
> **estática**, calculada una vez: una parte que se encogiera al atachar un volumen
> pondría en backpressure retroactivo a logs que ya estaban por encima de su parte nueva,
> que es un guest fallando WRITEs porque llegó *otro* volumen al host. Un Agent que no
> puede derivar una parte **no arranca** (`NewVolumeManager` rechaza `Budget.Share() <= 0`),
> y ésa es la diferencia entre esta cota y las dos de arriba: son configuración, y ésta es
> una precondición.

### 5.8 S3 es la autoridad de recovery (nuevo)

> El punto durable de un volumen = el final del **prefijo contiguo más largo** de sequences bajo `wal/<vol>/<epoch>/` para el **epoch fenced más alto**. Los watermarks en PostgreSQL son informativos (actualización lazy), nunca autoridad.

Corolario: PostgreSQL completo debe poder reconstruirse desde S3 (`rebuild-metadata`, §22.5). El layout en S3 es autodescriptivo.

> **La regla de arriba es V2; el corolario no.** En V1 el object store es la autoridad de
> **arranque**, no de recovery — es INV-08, y el cambio de palabra es el invariante entero:
> no hay prefijo contiguo que establecer ni epochs que ordenar, hay **un manifiesto y sus
> chunks** que resuelven o no resuelven (`image.Load`). No existe replay a media sesión que
> pudiera necesitar un punto durable. Los watermarks de PostgreSQL siguen siendo
> informativos por la misma razón de siempre. El corolario **sí** se cumple hoy, y es lo
> único de §5.8 que tiene mecanismo: §22.5.

### 5.9 Background siempre cede (nuevo)

Todo I/O del Agent pertenece a una clase (foreground / flush / background). El tráfico background (objectization, compactación, hidratación, prefetch, GC) opera con presupuesto fijo de NVMe y red, y cede ante foreground y flush. Ninguna mejora de background puede degradar la latencia del data path.

> **V2 con §11: no hay clases y no hay background que ceder.** Los cinco productores que
> esta regla nombra — objectización, compactación, hidratación de standby, prefetch y GC —
> los retiró ADR-0026, y con ellos el único I/O del Agent que no era del data path. Lo que
> queda fuera del camino del guest son **dos** cosas, no una: la publicación al parar
> (§14.8), que ocurre cuando el volumen ya no sirve a nadie, y —esto es lo que este banner
> se olvidaba— la subida de un **snapshot**, que corre en una goroutine *mientras el
> volumen sirve* (§19: ése es el punto, no hay pausa). O sea que sí hay I/O de background
> concurrente con el data path, sin presupuesto y sin clase, y la razón por la que no
> duele es que dura una vez por snapshot y compite por ancho de banda, no por el lock del
> volumen. Es el primer productor de §11 y ya está acá; el invariante vuelve cuando haya
> algo que ceder.

### 5.10 Nada sale del host en claro (nuevo)

Todo payload de datos de VM (WAL records, segmentos, checkpoints) se cifra con la DEK del volumen antes de cualquier PUT. Los metadatos estructurales (headers, manifests, descriptors) no contienen datos del guest.

### 5.11 El GC no puede causar el peor incidente (nuevo)

El GC marca; nunca ejecuta borrado permanente. El borrado real lo ejecuta el lifecycle del bucket sobre versiones no-actuales tras el grace period. Las credenciales del GC no incluyen `DeleteObject` permanente ni bypass de Object Lock.

> **No hay GC (ADR-0026 incremento 1), así que la mitad de este invariante que habla de un
> proceso de marcado no puede dispararse: es INV-14, `pending`.** La otra mitad —el
> corolario que importa— **sí se cumple, y está enforceada en la construcción**:
> `real.NewS3Store` se niega a usar un bucket sin versioning (`TestRequireVersioning`), y
> la interfaz `objectstore.Store` no tiene borrado permanente que ofrecer.
>
> **Corrección de 2026-08-08: la frase «nada en este repositorio elimina un objeto
> todavía» dejó de ser cierta y este párrafo la seguía afirmando.** `lineage.Delete`
> marca el descriptor de un volumen, los manifiestos de sus snapshots, su manifiesto y sus
> chunks, y lo maneja `control-plane -delete-volume`. Es exactamente la forma que §5.11
> pide y no la que prohíbe: cada borrado es un delete marker reversible y el borrado
> permanente lo hace —o no lo hace— el lifecycle del bucket, que **ningún código
> configura**. Ésa es la parte que sigue faltando.

---

## 6. Arquitectura de deployment

(Ver diagramas `diagrama_deployment_mvp.svg` y `diagrama_nodo_y_flujos_mvp.svg`; actualizar en v5 con: lease heartbeats CP↔Agent, KMS, standby tibio.)

### Comunicación

| Origen | Destino | Protocolo | Uso |
|---|---|---|---|
| Cliente | Control Plane | — | **No existe.** Los verbos son flags one-shot de `cmd/control-plane` (§7) |
| Control Plane | PostgreSQL | PostgreSQL/TLS | Metadata, leases, epochs, reconciliación |
| Control Plane | Volume Agent | — | **No existe, y es deliberado (ADR-0021).** El Agent no corre servidor |
| Volume Agent | Control Plane | gRPC/mTLS | **Heartbeat + renovación de lease + reporte de capacidad** |
| Volume Agent | KMS/Vault | HTTPS/mTLS | Unwrap de DEKs (solo en attach/recovery) |
| QEMU | Volume Agent | vhost-user Unix socket (reconectable) | I/O |
| Volume Agent | S3 | HTTPS/S3 API | WAL, checkpoints, imágenes, descriptors |
| Host | Otro host | Ninguno | No existe replicación directa |

> **Las dos filas marcadas «no existe» tenían la dirección al revés, y el código es el
> coherente.** Todo RPC en este sistema es **Agent → Control Plane**: el Agent late, pide
> su desired state y reporta; el Control Plane no llama a nadie y el Agent no escucha en
> ningún puerto. Es ADR-0021 —*el Agent es informado, nunca pregunta por permiso*— y tiene
> una consecuencia operativa que conviene decir: el trabajo administrativo llega a un host
> **cuando ese host vuelve a preguntar**, no cuando el operador aprieta enter. Un `-detach-volume`
> retorna apenas el catálogo cambió; que el Agent lo haya visto es el heartbeat siguiente.
>
> Del lado del cliente tampoco hay una API administrativa: cada verbo es un flag de
> `cmd/control-plane` que hace una cosa y termina (§7). El servidor Connect que sí existe
> es el que atiende a los Agents, **sin autenticación y escuchando en `:8080` por defecto**
> — con `GetVolumeKeys` encima, que devuelve la DEK envuelta de un volumen a quien sepa su
> id. Para cualquier despliegue que no sea una caja de desarrollo, eso es una red privada
> o un proxy delante; no hay interceptor en el árbol.

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

- Crear, eliminar y ~~**redimensionar**~~ (V2, §3) volúmenes.
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

> **Qué de esta lista tiene verbo hoy, y con qué se invoca.** El Control Plane de V1 no
> expone un cliente administrativo: cada verbo es un flag de `cmd/control-plane` que hace
> una cosa y termina, y es lo que `integration/e2e` maneja. Existen `-seed-volume`
> (crear), `-attach-volume` / `-detach-volume` (colocar y liberar; `-attach-volume` sin
> `-attach-host` pregunta a `placement.Choose`), `-snapshot-volume`, `-clone-snapshot`,
> `-rebuild-metadata` y `-fleet-status`, y desde entonces `-flatten-volume`,
> `-delete-volume`, `-cordon-host` y `-uncordon-host`. **Borrar sí existe** (la frase
> anterior decía lo contrario y una auditoría la corrigió el 2026-08-08):
> `-delete-volume` marca cada objeto del volumen y borra sus filas, llevándose con ellas
> la DEK envuelta, que es el crypto-shredding de §15.3 — con la salvedad de que un delete
> marker es reversible mientras el lifecycle del bucket no lo expire, y ese lifecycle no
> lo configura nada.
>
> De los demás puntos: las promociones y la espera de fencing son V2 (§12), el GC no
> existe (§21), el standby tibio no existe (§22.3), el drain se borró (§28.1 lleva lo que
> queda) y el aplanado de cadenas nunca se construyó (§20.1). El `request_id` sigue en
> cada mutación, pero no se materializa en ninguna tabla `operations` — ver §8.

### Modelo de reconciliación (nuevo)

Toda operación de larga duración (attach, detach, clone, drain, recovery, aplanado, resize) se representa como una fila con `desired_state` / `current_state`. Loops de reconciliación idempotentes y nivel-triggered convergen el estado. El CP puede crashear en cualquier punto: nada queda a medias, solo re-converge. La respuesta operativa ante incidentes es "arreglar la causa y dejar reconciliar", no cirugía manual en la base.

> **La forma sobrevive; la fila no.** V1 reconcilia de verdad — el Agent pregunta su
> desired state en cada ciclo y converge (`agent.Loop.Reconcile`, que late, lee el desired
> state y reporta; el `Loop.Apply` que este documento nombraba no existe), que es lo que
> hace que un snapshot pedido y un volumen despachado ocurran sin que nadie mande una orden
> — pero el estado deseado viaja en su **propio RPC** (`GetDesiredState`, cuyo
> `DesiredVolume` está en `api/spin/storage/v1/control_plane.proto`) y no dentro de la
> respuesta del heartbeat, como decía acá: son dos llamadas del mismo ciclo, no una. Esa
> tabla se retiró (§8), y con ella las operaciones que solo ella representaba: drain,
> recovery, aplanado y resize. Lo que queda reconciliado es lo que el desired state
> nombra: qué volúmenes sirve este host, y qué snapshot le falta tomar.

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

> **Este es el boceto de v5; el esquema declarado es `internal/schema/schema.sql`** y es la
> única fuente de verdad (ADR-0019: pgschema planifica contra él, sqlc genera desde él y el
> lane de integración construye su base desde él). Las diferencias que un lector notaría
> primero: los identificadores son `uuidv7` — un dominio sobre `uuid`, no `TEXT` (INV-22) —
> `volumes` lleva además `parent_snapshot_id`, `dek_key_id` y un CHECK que enforcea el orden
> de watermarks de §5.6, y `hosts` lleva la razón del cordon (§28.1). En `hosts`,
> `nvme_committed_bytes` **no es una columna**: lo comprometido se *deriva* sumando los
> volúmenes que el catálogo ya coloca en ese host (ADR-0017), porque un número que SQL puede
> recalcular es una caché y la caché más barata de mantener honesta es la que no existe. La
> columna que sí hay en su lugar es `nvme_remote_backlog_bytes`, que es lo contrario: se
> almacena porque nada la puede derivar. Y lo que **no** existe es la tabla `operations`.

```sql
CREATE TABLE volumes (
    volume_id          TEXT PRIMARY KEY,
    size_bytes         BIGINT NOT NULL,          -- INMUTABLE en V1: no hay resize (§3)
    -- Sin columna `durability`: ADR-0026 retiró el par 'remote'/'local' y con él la
    -- columna, en vez de dejarla por defecto en 'remote' — un catálogo que dice
    -- 'remote' afirma una durabilidad que el data path ya no da, y el que la lee no
    -- tiene cómo saber que es decoración. El esquema declarado real es
    -- `internal/schema/schema.sql` (líneas 96-102), que escribe esta misma razón.
    block_size         INTEGER NOT NULL,          -- bloque lógico del guest, NO 64 KiB (DEV-0024)
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

> **Esta tabla no existe (retirada en 2026-08-05).** Se deja aquí porque §7, §18 y §28.1 la
> nombran y un lector tiene que poder resolver la referencia, no porque describa el esquema.
> Su vocabulario de `kind` es el inventario de lo que se fue: `resize` es V2 (§3),
> `drain`, `recovery` y `flatten` describen mecanismos que ADR-0026 retiró, y `gc` no
> existe. Después de eso nada escribía una fila — los dos escritores (`RecordOperation`,
> `UpdateOperationPhase`) tenían por único llamador su propia prueba — y se borró en lugar
> de dejarla vacía: una tabla vacía con tres índices, un vocabulario y un escritor
> term-guarded se lee como un mecanismo que alguien está por usar, y el lector siguiente no
> tiene cómo saber que no. La razón está escrita en la cabecera de
> `internal/schema/schema.sql`, que es donde vive el esquema real.

```sql
CREATE TABLE operations (          -- reconciliación general — NO EXISTE, ver arriba
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

> **El layout de tres dispositivos está FUERA DEL ALCANCE de este módulo (decidido
> 2026-08-08).** Una auditoría doc↔código encontró trece claims rojas que salían todas de
> acá: no hay EROFS, no hay efímero, no hay overlay, no hay Docker ni containerd, y
> `grep -rn 'erofs|overlay|lowerdir|/dev/vdb|/dev/vdc|ephemeral'` sobre `internal/ cmd/
> integration/` no encuentra nada. No estaba a medio construir: no estaba empezado.
>
> **Y no le corresponde.** `storage` sirve **un** dispositivo de bloques remoto por
> vhost-user-blk. Qué discos ve una VM, cuál es su raíz, dónde monta el overlay y sobre qué
> disco vive Docker lo compone quien arranca la VM —`spin`/`spinbox`, ADR-0021— que es el
> que ya tiene la imagen base, el kernel y la línea de QEMU. Meter el layout acá sería un
> módulo nuevo entero y contradiría esa ADR.
>
> Lo que este documento describe en §1, §3, §9 y §31 sobre los tres dispositivos es el
> diseño del **sistema completo**, y se conserva como tal: es el contexto que explica por
> qué el volumen persistente se parece a lo que se parece. Pero no es una promesa que
> `storage` pueda cumplir ni fallar, y contarla como pendiente hacía que trece renglones
> del criterio de éxito fueran permanentemente rojos por algo que nadie iba a construir
> acá. La frontera es exacta: **de `/dev/vdb` hacia adentro es de este módulo; qué otros
> discos hay al lado, no.**

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
- `VIRTIO_BLK_F_DISCARD` y `VIRTIO_BLK_F_WRITE_ZEROES`: el espacio liberado por el guest se
  recupera (ver §14.6). Montar ext4 con `discard` o correr `fstrim` periódico.
  **Anunciados desde 2026-08-08.** Antes no lo estaban, y ésa era la forma más cara de
  hueco que tiene este repositorio: `wal.Log.Discard`, `cow.IntervalMap.Clear`, el codec de
  `RecordDiscard` y la lectura de ceros estaban completos y probados, y **ningún guest
  podía pedirlos**, porque un driver no manda un request de una feature que el dispositivo
  no anuncia. Faltaba un bit. Lo que se agregó con él: el parseo del array de
  `struct virtio_blk_discard_write_zeroes` (son rangos múltiples, no uno), los seis campos
  de configuración —`max_discard_sectors` en cero es una feature que el guest negocia y no
  usa nunca, en silencio— y `write_zeroes_may_unmap = 1`, porque este dispositivo siempre
  libera el rango. La prueba es del lado del kernel: `spin.mode=discard` en
  `integration/guestinit` emite `BLKDISCARD`, que el block layer sólo manda si la feature
  se negoció.
- ~~Resize: el grow se propaga vía actualización del config space + notificación; el guest
  expande con `resize2fs`.~~ — **V2 (§3).** Esta frase es la mitad que nunca se construyó:
  la notificación es `VHOST_USER_BACKEND_CONFIG_CHANGE_MSG`, que viaja por el canal de
  peticiones del backend hacia el front-end, y `vhost.ProtocolFeatures` no anuncia ese canal
  (`VHOST_USER_PROTOCOL_F_BACKEND_REQ`): ofrece `REPLY_ACK` y `CONFIG`, y nada más — el
  comentario sobre esa constante dice de cada bit por qué está o no está. Sin el canal no hay
  forma de decirle al guest que
  su disco creció, y un `resize2fs` sobre una capacidad que el dispositivo no movió no tiene
  nada que expandir.

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
lease_ttl: 10s                  # lease POR HOST (§12.6) — real: 30s (-lease-ttl)
lease_renewal_interval: 3s      # en el heartbeat de capacidad — real: 5s (-heartbeat-interval)
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

> **Lo que `cmd/volume-agent` acepta de verdad es otra lista**, y el bloque de arriba
> describe sobre todo V2 (batches, checkpoints, clases de I/O). Los flags que existen:
> `-host-id`, `-control-plane`, `-data-dir`, `-vhost-socket-dir`, `-heartbeat-interval`,
> `-retry-backoff`, `-lease-ttl`, `-rpc-timeout`, `-otlp-endpoint`, `-shutdown-grace`,
> `-kek-file` y **`-max-volumes`** — el divisor del presupuesto del dispositivo (§5.7) —
> más los seis del object store, que esta lista se olvidaba y uno de los cuales es
> **obligatorio**: `-s3-bucket`, `-s3-endpoint`, `-s3-region`, `-s3-create-bucket`,
> `-s3-access-key` y `-s3-secret-key`. Son los mismos que toma `cmd/control-plane`
> (`internal/storecfg`), y que los dos binarios apunten al mismo bucket no lo verifica
> nadie: el heartbeat no lleva el bucket, así que dos mitades apuntadas a lugares
> distintos no dan error — dan un despliegue vacío.
> Tres cosas más que ese bloque no dice y que un operador necesita: sin `-kek-file` el Agent
> escribe datos de guest **sin cifrar** y lo advierte por log (es modo dev, §15); el
> `lease_ttl` de acá es liveness, no durabilidad (§14.8); y `-shutdown-grace` acota **un
> intento** de publicación, no la espera entera (§14.8).

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

## 11. Clases de I/O internas (nuevo) — V2

> **Retirada con ADR-0026 (2026-08-02), y no reescrita.** `internal/ioclass` no existe, no
> hay token buckets y no hay nada que priorizar: la clase *background* enumeraba
> objectización, compactación, hidratación, prefetch, GC y materialización, y ninguna
> existe; la clase *flush* eran los PUTs que bloqueaban un ACK, y un ACK ya no espera
> ningún PUT (§14.8). Queda el data path del guest, que es todo el I/O del Agent mientras
> un volumen sirve. Las dos métricas de abajo se fueron con el mecanismo (§26.2, DEV-0022).
> El texto está en git y vuelve con la durabilidad remota.

Todo I/O del Agent pertenece a exactamente una clase:

| Clase | Contenido | Política |
|---|---|---|
| **foreground** | Data path del guest: WAL append, `fdatasync`, read misses | Prioridad absoluta en NVMe |
| **flush** | PUTs que bloquean ACKs de FLUSH/FUA | Prioridad absoluta en red |
| **background** | Objectization, compactación, hidratación de standby, prefetch, GC, materialización | Presupuesto fijo (token bucket por recurso); cede siempre |

Implementación mínima: token buckets por clase para NVMe (IOPS + bytes) y para red (bytes), en el propio Agent. Sin esto, cada proceso de background es una fuente de incidentes de latencia; con esto, son invisibles.

Métricas: `io_class_bytes_total{class,resource}`, `io_class_throttled_seconds{class}`.

---

## 12. Fencing (V1)

> **Reducido en 2026-08-02 por ADR-0026.** Lo que había aquí — ciclo de lease, promoción,
> `FENCING_WAIT`, objeto de epoch con CAS, PUTs tardíos del writer viejo, escalabilidad del
> lease por host — describía un protocolo que gobernaba **cada ACK durable**. V1 no ACKea
> nada contra S3: el ACK es `fdatasync` local (§14.8) y el volumen sube al parar. El
> protocolo completo está en git y vuelve con la durabilidad remota, que es V2.

V1 conserva **una** obligación de fencing, y es la única que sigue siendo capaz de perder
datos en silencio:

**Dos incarnaciones de un volumen no pueden publicar las dos.** Si dos hosts arrancan el
mismo volumen y ambos suben su imagen al parar, el segundo manifiesto sustituye al primero
y las escrituras de aquella sesión desaparecen sin error en ninguna parte.

El mecanismo es un **compare-and-set sobre el manifiesto**: un host publica contra el ETag
del manifiesto que leyó al arrancar, y un manifiesto que se movió por debajo le rechaza la
publicación (`image.ErrSuperseded`). Un host que nunca leyó ninguno publica en modo
create-only, así que pierde contra el que sí lo leyó.

Los chunks son direccionados por contenido y create-only, de modo que dos escritores que
produzcan los mismos bytes no pueden corromperse entre sí — la clave *es* el contenido.

## 13. Modelo CoW

### 13.1 Granularidades desacopladas (cambio clave vs v4)

- **WAL**: registra el **extent real** del write del guest (offset + length, alineado a 512 B). Un write de 4–16 KiB loguea 4–16 KiB, no 64 KiB. El read-modify-write desaparece del data path.
- **Segmentos CoW**: *retirados.* No hay grilla en ninguna parte, y no hay RMW que pagar — ni en el data path ni en background. Lo que sale del host es un chunk que **es un extent** del volumen, cortado solo donde un extent excede `image.MaxChunkBytes` (64 MiB).

Esto reduce simultáneamente: bytes de WAL local, bytes subidos a S3, tiempo de replay y costo — precisamente para el peor workload (bases de datos con páginas de 8–16 KiB y fsync frecuente).

> **DECIDIDO 2026-08-08 (DEV-0024, cerrado): la granularidad CoW de 64 KiB se retira, no
> se aplaza.** El primer bullet es exacto y es lo mejor de v5; el segundo describía una
> estructura que nunca existió, y la alternativa que la haría existir se examinó y se
> rechazó: cortar chunks sobre una grilla fija haría los límites independientes de la
> historia de escritura, pero una celda escrita a medias hay que completarla desde la
> ancestría — o sea, leer a través de la cadena **al publicar**, que es exactamente el
> aplanado que la decisión de `chain_depth` quitó, reapareciendo una capa más abajo y por
> celda. Guardar celdas parciales devuelve los límites a donde ya están.
>
> Lo medido, y está fijado por `TestChunksAreExtentSizedAndDedupByContentAlone`: un write
> de 512 B produce un chunk de 512 B; dos volúmenes de un linaje con el mismo contenido
> comparten **un** objeto aunque lo hayan escrito en offsets distintos, porque la clave es
> el digest del contenido y el offset vive en el manifiesto; y dos writes de 4 KiB
> adyacentes se funden en un extent de 8 KiB. La debilidad, dicha en voz alta: la dedup
> depende de que **coincidan los límites de extent**, así que dos volúmenes que escriben lo
> mismo partido distinto no comparten nada. Eso no cuesta nada en el caso que V1 tiene —un
> padre con la imagen y clones con deltas encima, cuyos chunks se heredan— y lo que lo
> arreglaría es un chunker definido por contenido (rolling hash), que es V2 y cuyo
> disparador es un workload, no un tamaño: varios volúmenes escribiendo **el mismo**
> contenido con límites **distintos**.
> No hay segmentos CoW de 64 KiB en ninguna parte: `cow.IntervalMap` trabaja
> sobre los extents reales y nada los alinea a una grilla — el propio comentario de esa
> estructura dice que la segunda que prometía el incremento 4.4 no llegó. Lo que la
> reemplazó tiene otras propiedades y vale conocerlas: lo que sale del host son **chunks de
> hasta 64 MiB** (`image.MaxChunkBytes`) direccionados por el digest de su texto plano, así
> que una región que no cambió se saltea entre paradas y **no hay RMW en ningún lado**,
> ni en el data path ni en background. El tamaño se eligió por los dos extremos: un objeto
> por volumen choca contra el límite de PUT del backend y re-sube todo en cada parada; un
> objeto chico multiplica requests.
>
> **`block_size` no es esta granularidad**: es el tamaño de bloque lógico que se le
> reporta al guest, validado como múltiplo del sector de 512 B, y `control-plane
> -seed-block-size` lo default-ea en 4096. El comentario del esquema declarado que decía
> lo contrario se corrigió el 2026-08-07.

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

> **La cadena de arriba tiene tres eslabones que no existen.** El read path de V1 es de dos
> capas y una constante: los extents de esta sesión en el interval map del volumen
> (`cow.IntervalMap`), debajo la base que se cargó al atachar desde el manifiesto
> (`image.Load`, instalada con `SetBase`), y ceros para todo lo que ninguna de las dos
> cubre — `IntervalMap.Read` los escribe, no hay "zero block" que buscar. No hay checkpoint,
> no hay segmento local objectizado y **no hay read-miss contra S3**: el object store se lee
> una vez, al atachar, y no vuelve a aparecer en el camino de una lectura. La frase del
> cross-host sigue siendo cierta en su forma (un clon en otro host descarga completo), pero
> ni el standby tibio ni el lazy loading existen para mitigarla; lo que la mitiga es colocar
> el clon donde ya están sus datos (§20, `placement.Choose`).

### 13.3 Active map con cota de memoria

Con segmentos de 64 KiB, 1 TiB = 16M entradas posibles. Implementación: roaring bitmaps (presencia) + tabla de ubicaciones por rangos, no hashmaps naive. Métrica `active_map_bytes` por volumen; entra en el presupuesto de memoria del Agent (§10.1).

> **El active map se construyó y se borró el 2026-08-02 sin haber tenido nunca un usuario**
> (§17 lo dice donde se escribe un WRITE). No hay roaring bitmaps y no existe
> `active_map_bytes`. La estructura que sí tiene la cota que esta sección pide es
> `cow.IntervalMap`, y desde 2026-08-06 se mide: `read_view_bytes`, `read_view_extents` y
> `read_view_layers` (§26.2). El problema que §13.3 planteaba sigue siendo real —es la única
> estructura por volumen cuyo tamaño lo decide el guest y no la configuración— y hasta que
> esas tres series existieron, la primera evidencia de un host con demasiadas vistas de
> lectura habría sido el OOM killer.

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

> **§14.2 a §14.5 son V2 en bloque (ADR-0026, 2026-08-02), y no están reescritas.**
> Describen el WAL remoto: el objeto batch y su header, las reglas de cierre y PUT, los seis
> pasos del FLUSH y la idempotencia de esos PUTs. No hay nada de eso en el árbol — no existe
> batcher ni uploader, `wal.Log` no tiene object store, y el paso durable es `fdatasync` y
> avanzar el watermark (`Log.durableStep`). El contrato vigente es **§14.8**, y §17 apunta
> ahí desde el camino del guest. **Lo que sí sobrevive de este bloque**, en otra forma: la
> idempotencia de §14.5 es estructural (INV-21 — la clave de un chunk *es* el digest de su
> texto plano, así que una clave que ya existe se saltea), y la clave determinística sigue
> siendo la idea que hace que reintentar sea seguro. §14.1 (el record) y §14.6, §14.7 y
> §14.8 no son V2: son el formato y el camino que V1 usa.

### 14.2 WAL Object (batch remoto) — V2

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

### 14.3 Reglas de cierre y PUT de batch (reemplaza la regla de 50 ms) — V2

La durabilidad solo se promete en FLUSH/FUA; no hay razón para subir batches por timer corto. Un batch se cierra y se programa para PUT cuando se cumple **cualquiera** de:

1. **FLUSH o FUA recibido**: cierre inmediato (aunque tenga 1 record). Los PUTs pendientes se suben **en paralelo** (pipeline bajo demanda).
2. **Tamaño objetivo**: `current_batch_bytes >= 8 MiB`.
3. **Tamaño máximo**: `>= 16 MiB` (hard limit).
4. **Antigüedad**: primer record con más de **20 s** (colchón contra `max_unflushed_age = 30 s`).
5. **Snapshot solicitado**: cierre y durable hasta `target_sequence` (en background, §19).
6. **Límite de unflushed alcanzado**: cierre forzado para liberar presión.
7. **Shutdown limpio**.

Efecto: un volumen sin fsyncs frecuentes pasa de ~20 PUTs/s (v4) a ~3 PUTs/min. La cardinalidad de objetos y el costo de requests bajan 2-3 órdenes de magnitud. Métricas `wal_batch_size_bytes` y `wal_small_batch_ratio` vigilan el caso fsync-pesado, que se corrige con compactación (§21.2).

### 14.4 Orden de operaciones en FLUSH / FUA (crítico) — V2

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

### 14.5 Idempotencia de PUT — V2

Idéntica a v4: clave determinística por `(volume_id, epoch, first_seq, last_seq, content_hash)`, `If-None-Match: *`, y retry + HEAD + checksum ante respuesta perdida. Mismo rango con hash distinto → fallo duro.

### 14.6 DISCARD y recuperación de espacio (nuevo)

Sin TRIM, los bloques borrados por el guest viven para siempre: el costo de S3 solo crece y la materialización cross-host descarga basura. Con DISCARD:

- El objectizer marca los segmentos/rangos descartados como ausentes en el active map.
- Los checkpoints y la compactación no reescriben rangos descartados.
- Los reads de rangos descartados devuelven ceros.

El storage converge al working set en lugar de crecer monotónicamente. Métrica: `discarded_bytes_total`.

> **Los tres pasos de arriba nombran maquinaria retirada; el efecto que importa se
> conserva.** No hay objectizer, ni checkpoints, ni compactación. Lo que un DISCARD hace en
> V1 es `cow.IntervalMap.Clear`: el rango sale de la vista de lectura, se lee como ceros, y
> —esto es lo que hace que el storage converja— **no se sube**, porque lo que se publica al
> parar es el **delta** de la vista sobre lo que el volumen heredó (`view.DeltaOver`), más
> tombstones explícitos para lo descartado — no `view.Ranges()`, que es lo que decía acá y
> lo que hacía el árbol antes de la cadena. El efecto sobre el espacio es el mismo y la
> diferencia importa igual: un rango descartado no se sube **y además** queda anotado como
> ausente, que es lo que impide que reaparezcan los bytes de un ancestro. O sea que el espacio se recupera
> en el object store en el momento en que este documento decía que se recuperaba en el
> objectizer. Lo que **no** se recupera es el byte local: el record de DISCARD se anexa como
> cualquier otro y no hay reclamo a media sesión (§5.7).

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

> **El layout real es más chico, y el epoch está en el path.** Lo que existe es
> `<data-dir>/wal/<volume-id>/<epoch>/<first-seq>.seg` — `wal.SegmentDir` lo construye y
> `segmentName` nombra el archivo, con la primera sequence zero-padded a ancho fijo para que
> el orden lexicográfico sea el orden de las sequences. El epoch va en el path espejando el
> layout de claves en S3, así que un directorio de otro epoch es **imposible** en vez de
> meramente detectable; el header del segmento repite las dos cosas, porque el path dice
> dónde se archivó el directorio y el header dice qué es. No hay `state.json` (el estado
> vive en el catálogo y en el manifiesto), ni `active.wal` (el segmento abierto es el último
> por sequence), ni `cache/`, ni `segments/`, ni `checkpoints/`. El directorio entero está
> bajo el `flock` que garantiza un Agent por host (DEV-0014).
>
> El corte de segmento no es 256 MiB fijos: `wal.Limits.SegmentBytes` sale de la parte del
> presupuesto del volumen (§5.7). Y la regla de no truncar por encima del punto publicado es
> INV-13, que **volvió a tener productor** en 2026-08-06: `wal.Log.InstallBase` desenlaza,
> al atachar, los segmentos que la imagen restaurada ya cubre. Sin eso un volumen arrancado
> y parado diez veces sobre un host se quedaba con diez sesiones de WAL bajo una cota que
> nada limpiaba.

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
7. **Un Agent que no puede publicar no para.** La regla 2 dice que S3 recibe el volumen al
   parar; ésta dice qué pasa cuando no puede, que es la mitad que este documento no tenía
   (revisada y decidida el 2026-08-04). El Agent
   sigue vivo, se queda con el lock de su data-dir, conserva el WAL local y reintenta con
   backoff **indefinidamente**; `-shutdown-grace` acota **un intento**, no la espera, y no
   es un presupuesto tras el cual se abandonan datos. Se eligió contra salir con código
   distinto de cero, que era lo que la spec proponía: un Agent que sale es
   indistinguible de uno que crasheó, y la flota ve un host muerto sin poder saber si
   guarda una sesión que nadie tiene. Uno que sigue arriba diciendo *estoy reteniendo
   datos sin publicar del volumen X, intento 14* es algo sobre lo que se puede actuar, y
   además sigue heartbeateando. **El costo, dicho sin adornos:** una caída del object
   store deja a cada Agent afectado vivo y negándose a parar, así que un rolling restart
   se cuelga a nivel flota hasta que el store vuelva. Es el lado correcto del trade: una
   flota que no reinicia durante una caída es un problema operativo con causa obvia; una
   que reinició y perdió una sesión por host es un problema de datos sin ninguna.
   Las salidas son tres: una **segunda señal** abandona y sale distinto de cero diciendo
   qué sesiones deja sin publicar (`agent.ErrPublishAbandoned`, que `exitCode` traduce);
   un `SIGKILL` libera el flock y **no pierde nada**, porque los records están en disco y
   el Agent siguiente sobre ese host re-atacha en el mismo epoch y los publica (ADR-0024);
   y que el store vuelva, que es el caso para el que existe. La excepción que sí sale es
   `image.ErrSuperseded` — otro escritor publicó encima, reintentar sería pisar una imagen
   más nueva con una más vieja, que es exactamente lo que INV-10 impide. **No es la única
   que no se reintenta**, y decir «la única» ocultaba la otra: `agent.ErrNoReadView` cae en
   la misma rama terminal, y significa algo distinto —este Agent nunca llegó a tener una
   vista de lectura, así que no tiene una imagen que publicar—. Las dos comparten que
   reintentar no puede mejorar nada; sólo la primera es una carrera contra otro host.

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

> **Mitad retirada por ADR-0026 y no reescrita.** De los estados de abajo, V1 recorre
> `DETACHED → ATTACHING → ACTIVE` y `ACTIVE ⇄ SNAPSHOTTING`. Los otros cuatro
> —`SELF_FENCED`, `FENCED`, `RECOVERY_REQUIRED`, `RECOVERING`— no tienen transición que los
> alcance: `SELF_FENCED` era el lease gobernando el ACK y un ACK no consulta el lease
> (§14.8); `RECOVERING` era la recuperación a media sesión, que no existe.
>
> **Corrección de 2026-08-09: en el árbol no hay ninguna máquina de estados por volumen.**
> Este banner decía que el vocabulario «sigue en `internal/lifecycle` y en el CHECK del
> esquema»; lo que vive ahí es el vocabulario de *propiedad* del Control Plane (§7), sobre
> los mismos seis valores del CHECK, y `ATTACHING`, `SNAPSHOTTING`, `SELF_FENCED` y
> `FENCED` no existen como símbolo en ninguna parte — `agent.Volume` no tiene campo de
> estado. Lo que un Agent realmente recorre no es un autómata: es «este volumen está en mi
> desired state y lo estoy sirviendo» o no lo está. La secuencia de abajo sigue siendo una
> descripción útil de *lo que pasa al atachar*, y ahí es donde hay que leerla; como
> máquina de estados, no tiene implementación.
>
> **De la secuencia ATTACHING → ACTIVE**, los pasos 3 y 4 (cargar checkpoint, reproducir
> WAL local + remoto hasta el punto durable) son **un** paso: leer el manifiesto del volumen
> y sus chunks (`image.Load`) e instalarlo como base del interval map. El paso 1 obtiene un
> lease que es liveness, no permiso de escritura. El resto —unwrap de la DEK, abrir el
> socket vhost-user, publicar ACTIVE— es exacto.
>
> Lo que esta sección **no** tiene y el árbol sí es el otro extremo: qué pasa al parar
> cuando la publicación falla. Está en §14.8, regla 7.

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
8. Publicar ACTIVE. — **no ocurre:** la fila se escribe `ACTIVE` al aprovisionar y no se
   mueve; `SetVolumeState` no tiene llamador de producción. Lo que la flota observa como
   «este volumen está siendo servido» son las watermarks que el heartbeat reporta, no una
   transición de estado.

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
→ actualizar el interval map (`cow.IntervalMap`; el active map de §13.3 se construyó
  y se borró el 2026-08-02 sin haber tenido nunca un usuario)
→ ACK
```

Puede perderse si el host se pierde antes de FLUSH/FUA/snapshot (write-back anunciado).

### WRITE + FUA / FLUSH

```text
Guest FLUSH/FUA
→ fdatasync local
→ ACK
```

Ver **§14.8**, que es el contrato vigente. §14.4 describe los seis pasos de V2 — cerrar el
batch, PUTs verificados, verificación de lease — retirados por ADR-0026. No hay nada del
object store ni del lease en este camino.

### DISCARD / WRITE_ZEROES

```text
Guest DISCARD
→ record sin payload en WAL local
→ actualizar mapas (rango ausente / ceros)
→ ACK
```

> **Corregido por ADR-0026 (2026-08-02); decía lo contrario de §14.8 dentro de este mismo
> archivo.** Un write cubierto por FLUSH/FUA sobrevive a la caída del proceso, del Agent y
> de QEMU. **No sobrevive a la pérdida del host**: ése es el RPO de una sesión que declara
> §2. Y lo que §12 garantiza hoy no es eso — es que dos incarnaciones del mismo volumen no
> publiquen las dos al parar.

---

## 18. Idempotencia

- PUTs de datos: clave determinística + `If-None-Match: *` + retry/HEAD/checksum (§14.5).
- Operaciones administrativas: `request_id` UUID único en **todas** las mutaciones (attach, detach, snapshot, clone, ~~resize~~ (V2, §3), ~~drain, recovery~~), ~~materializado en la tabla `operations`~~ — esa tabla no existe (§8). En V1 la idempotencia administrativa que queda es la del snapshot: su manifiesto se escribe create-only, así que un reintento converge en el suyo y rechaza uno distinto con el mismo id (`image.ErrSnapshotExists`, INV-16).
- Publicaciones de baja frecuencia (checkpoint, manifest): CAS contra el objeto de epoch (§12.4).
- Mismo rango con hash distinto → corrupción o divergencia → fallo duro.

> **Esta regla no tiene sujeto en V1, y el código es el coherente.** Presupone una clave
> que nombra un *rango* —`(volume_id, epoch, first_seq, last_seq)`— de modo que dos
> contenidos distintos puedan reclamar la misma. El almacenamiento es direccionado por
> contenido: la clave de un chunk **es** el digest de su texto plano, así que «mismo rango,
> hash distinto» son dos claves distintas y no hay colisión que detectar. La divergencia se
> volvió imposible en vez de detectable, que es más fuerte, y la verificación que sí corre
> es la de la lectura: un chunk cuyo contenido no hashea a su propia clave se rechaza.
> El fallo duro que queda es el del manifiesto, y es el CAS (§12).

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

Same-host: nuevo active child + efímero.

> **Lo de «sin descarga» no es cierto y el código es el coherente; es lo más caro que
> quedó abierto de la auditoría.** `placement` sí prefiere el host de origen, pero al
> atachar, `agent.parentChain` + `image.Load` bajan del object store **todos** los chunks
> de la ancestría, en el host que tomó el snapshot exactamente igual que en cualquier
> otro. No hay caché local de datos que reutilizar (§5.4): el WAL de la sesión anterior se
> publicó y se desenlazó al parar, y no hay nada más en disco.
>
> O sea que hoy la preferencia de placement **no compra nada medible**, y tres lugares de
> este documento la contaban como comprada: la fila de SLO de §2, el objetivo 6 de §3 y el
> criterio 6 de §31. Se corrigen los tres. Lo que la haría real es una caché de chunks por
> host indexada por digest —que el direccionamiento por contenido hace casi trivial: la
> clave ya es el digest, así que un chunk que ya está en disco es un `GET` que no hace
> falta— y no está construida. Es el incremento que le devuelve el sentido a §20, y hasta
> que exista, «boot rápido same-host» es una intención del diseño, no una propiedad del
> sistema.

Cross-host: descarga checkpoint + WAL posteriores; materializa completo; arranca. (Standby tibio y, después, lazy loading acortan esto; §22.3–22.4.)

### 20.1 Aplanado de cadenas (nuevo)

Cada clone-de-clone encadena checkpoints + WAL; el replay y el read path degradan linealmente con la profundidad. Regla:

```text
si chain_depth > max_chain_depth (5)  o  replay_estimado > umbral:
    programar FLATTEN (operación reconciliada, clase background):
    materializar un checkpoint "full" autocontenido para ese linaje
```

Equivalente al `flatten` de RBD / compactación de niveles de un LSM. Sin esto, el sistema funciona en la demo y degrada en silencio durante meses. Métricas: `chain_depth{volume}`, `flatten_operations_total`.

> **Este párrafo describía el árbol de antes de la decisión de `chain_depth`, y una
> auditoría doc↔código lo encontró falso en seis puntos el 2026-08-08.** Se reescribe
> entero. Lo que decía —que no hay FLATTEN, que ningún camino se niega por profundidad, que
> `max_chain_depth` no lo lee nadie, que `image.uploadChunks` recorre `view.Ranges()`, que
> el re-subido es la única razón por la que un clon de profundidad 2 no lee ceros, y que
> `chain_depth` no describe un camino de lectura— era cierto cuando se escribió y dejó de
> serlo cuando se construyó la cadena. **Lo que hay:**
>
> - **Hay techo.** `controlplane.MaxChainDepth = 5` es una constante de Go, no una clave de
>   configuración, y `controlplane.Clone` se niega **antes** de colocar cuando el clon
>   quedaría por encima (`ErrChainTooDeep`), diciendo en el error que la salida es aplanar
>   el padre.
> - **Hay FLATTEN**, y es un one-shot de operador: `control-plane -flatten-volume`. No es la
>   operación reconciliada en background que pide el bloque de arriba, y esa diferencia es
>   deliberada — no hay tabla `operations` (§8), así que no hay dónde vivir el progreso.
> - **La publicación ya no aplana.** `image.uploadChunks` recorre
>   `view.DeltaOver(inherited)`: escribe sólo lo que las capas propias de este volumen
>   afirman, más tombstones explícitos, bajo el prefijo de la raíz del linaje.
> - **La cadena se recorre al atachar** (`agent.parentChain`) y se compone en la vista de
>   lectura, de modo que un clon de profundidad 2 lee los bytes de sus ancestros porque los
>   *lee*, no porque alguien se los haya vuelto a copiar. Con eso, `chain_depth` describe el
>   camino de lectura tanto como el linaje, y `read_view_layers` (§26.2) mide la otra mitad.
> - `flatten_operations_total` sigue sin existir.

---

## 21. Objectization, compactación y GC — V2

> **Retirados en 2026-08-02 por ADR-0026.** Describían la cadena de durabilidad remota:
> objectization y truncado a mitad de sesión (§21.1), compactación (§21.2), GC
> mark-and-sweep (§21.3), determinación del punto durable desde S3 (§22.1), standby tibio
> (§22.3) y lazy loading (§22.4). **`rebuild-metadata` (§22.5) no**: se fue con ellos y
> volvió el 2026-08-03, en una forma mucho más chica — ver §22.5.
>
> V1 no tiene nada de eso: un volumen es una imagen que se publica al parar (§14.8), su
> WAL local vive una sesión, y arrancar es leer un manifiesto. El texto completo está en
> git y vuelve con la durabilidad remota.
>
> Lo que **no** se fue con ellos: §19 (un snapshot es un número, no un evento) y §20
> (clonado, localidad y cadenas), que son los dos que más peso cargan en V1.

## 22. Recovery y standby — V2 (la reconstrucción, §22.5, no)

Retirada con §21 por ADR-0026, y con la misma razón — **salvo §22.5**, que volvió. Las
subsecciones que el código todavía cita se resuelven aquí:

- **§22.1** — determinación del punto durable con autoridad S3. V1 no la necesita: no hay
  prefijo contiguo que establecer, hay un manifiesto que resuelve o no.
- **§22.3** — standby tibio. Requiere durabilidad remota continua.
- **§22.4** — lazy loading. Diseñado, no implementado, y ahora tampoco necesario: un clon
  en el host de origen no descarga nada (§20).
- **§22.5** — `rebuild-metadata`, reconstrucción de PostgreSQL desde S3. **Es la única
  subsección de §22 que sigue viva, y este párrafo decía lo contrario hasta 2026-08-07.**
  La implementación que ADR-0026 borró volvió el 2026-08-03, mucho más chica:
  `controlplane.RebuildMetadata` lee **dos objetos por volumen** — el `descriptor.json` que
  el volumen escribe al aprovisionarse y los manifiestos de snapshot bajo su prefijo — sin
  cadena de epochs, sin recovery-point y sin prefijo contiguo que reensamblar. Su llamador
  de producción es `control-plane -rebuild-metadata`, y es el lector que `descriptor.Write`
  venía alimentando sin tener ninguno. **No usa material de claves**: un operador con el
  bucket y sin la KEK puede reconstruir el catálogo, aunque no leer un byte de guest.
  Es INV-20, y su arma DST (`a-rebuilt-catalog-can-serve-its-volumes`) no comprueba que
  volvieron las filas sino que un Agent nuevo, sobre un data-dir que nunca vio el volumen,
  sirve los bytes que el guest original escribió. **Lo que no vuelve** está dicho donde se
  decide, en `volumeFromDescriptor`: el placement, porque ningún objeto lo registra — un
  catálogo reconstruido describe volúmenes que nadie está sirviendo, y ésa es la respuesta
  honesta. Dos hechos cambiaron de sentido con ADR-0026 y valen aquí: el `current_epoch`
  del descriptor es **autoritativo** (el objeto de epoch al que antes cedía se borró con la
  cadena de fencing), y la existencia del manifiesto de un snapshot **es** su estado
  PUBLISHED, porque se escribe create-only y no cambia (INV-16).

## 23. Edge cases

> **Mitad retirada por ADR-0026 (2026-08-02), y no reescrita.** Los casos que describen la
> cadena de durabilidad remota describen **V2**, y hasta que vuelva contradicen §14.8:
> *S3 caído* (en V1 S3 no está en el camino de ningún ACK — §14.8, INV-18 — así que un
> FLUSH completa con el backend caído y lo que falla es parar y publicar), *PostgreSQL
> caído* (ningún FLUSH depende del lease, y los modos `remote`/`local` que su excepción
> cita no existen), *WAL remoto subido / manifest no publicado* (no hay WAL remoto ni GC),
> y *Old writer vuelve* (no hay lease que vencer ni checkpoints que rechazar; queda el CAS
> del manifiesto al parar, §12).
>
> Siguen vigentes tal cual: *PUT exitoso con respuesta perdida* y *Manifest publicado /
> PostgreSQL no actualizado*. **Corrección de 2026-08-07: *Crash del Agent con requests en
> vuelo* estaba en esta lista y no corresponde** — dice que el Agent recupera las requests
> por inflight shmfd "verificado por DST", y no hay nada que recuperar ni nada que lo
> verifique: el incremento 3.3 no se empezó, `vhost.ProtocolFeatures` no anuncia
> `INFLIGHT_SHMFD` a propósito, y `inflight_recovered_total` es una serie sin productor. Es
> el caso de esta sección con más distancia entre lo que promete y lo que hay, y estaba
> bendecido como vigente. Ver §2. *NVMe lleno*
> conserva del 3 en adelante — sus dos primeros pasos nombran la objectización y los
> batches pendientes. *Reloj con deriva excesiva* conserva la alerta y pierde la
> inelegibilidad para promoción, porque nada promociona.

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

Inflight shmfd: al reiniciar, el Agent recupera y completa/reintenta las requests sin duplicar ni perder (verificado por DST). — **Nada de esto existe; ver el banner de esta sección y §2.**

---

## 24. Cliente S3 como subsistema (nuevo) — V2

> **No existe el subsistema, y la razón por la que no duele es §14.8.** Esta sección
> presupone que el object store está en el camino de cada FLUSH: por eso pide hedged GETs,
> presupuesto global de retries, circuit breaker y límites de ancho de banda por clase. En
> V1 el store se toca en dos *momentos* por sesión (que no es lo mismo que dos requests:
> atachar es un HEAD/GET del manifiesto más un GET por chunk de la ancestría, y parar es un
> PUT por chunk nuevo más el CAS) —al atachar, para leer el manifiesto, y al parar,
> para publicarlo— así que no hay latencia de cola del data path que recortar. El cliente
> real es un archivo detrás de `objectstore.Store` (`internal/simio/real/s3.go`, ADR-0010),
> sin hedging, sin circuit breaker y sin clases; el único mecanismo de reintento que existe
> es el del párrafo 7 de §14.8, y es al parar. Las métricas `s3_*` de §26.2 siguen
> declaradas porque el cliente sí puede reportar errores y latencia.
>
> Lo que **sí** es requisito hoy y vive en §6.1 es la semántica, no el rendimiento: CAS
> (`If-Match`/`If-None-Match`) y versioning, verificados por `task backend:conformance`
> antes de habilitar un backend.

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

### 25.5 Alcanzabilidad desde un binario (agregada 2026-08-07)

Nada de §25 detecta el defecto que este repositorio más veces se comió: una pieza
completa, con tests unitarios, property tests y escenario DST, **que ningún binario
llama**. Un checker verifica lo que el escenario ejerce, y un escenario no ejerce lo que
nadie invoca; la maquinaria sofisticada alrededor de un camino desconectado no hace que el
camino funcione, hace que el hueco sea más difícil de notar. Por eso el gate incluye
`task deadcode`: análisis estático sobre el programa construido, que responde qué símbolos
no alcanza ningún `main`.

Su forma importa más que la herramienta, y es un **trinquete de dos listas**, no un
contador. `hack/deadcode-allow.txt` es "nadie lo alcanza y está bien — para siempre" (una
afordancia de test, un modelo de referencia); `hack/deadcode-pending.txt` es "nadie lo
alcanza y es un defecto que nadie terminó de borrar". El trinquete gira en un solo sentido
y los dos lados fallan: un hallazgo que no está en ninguna lista pone el gate en rojo, así
que el conjunto no puede crecer — que es la propiedad que un *número* pineado no tiene,
porque con un número borrar un hallazgo compra el derecho a agregar otro y nadie nota el
canje — y una entrada que la herramienta deja de reportar **también** lo pone en rojo, así
que cuando el código se va, la línea se va en el mismo commit.

La consecuencia sobre qué significa "hecho" es directa y es la razón de que esto viva en
§25 y no en un README: **un componente sin llamador no es progreso, es pasivo**, y ahora
hay algo que lo dice sin depender de que alguien se acuerde.

---

## 26. Observabilidad y trazabilidad

### 26.1 Tracing distribuido (nuevo)

`request_id`/`operation_id` se propagan como trace context (OpenTelemetry) por CP → Agent → cliente S3 → KMS. Logging estructurado con esos campos en todos los componentes. El día que un FLUSH tarde 4 s, el MTTR es "abrir el trace", no "grep en tres hosts". Decisión de librería al inicio; costo marginal.

### 26.2 Métricas

> **Recortado con ADR-0026 (2026-08-03).** La taxonomía anterior declaraba unas cuarenta
> series y tres cuartas partes nombraban mecanismos que ADR-0026 retiró: los lotes y PUTs
> del WAL remoto, los checkpoints, la objectización y compactación, el GC, la espera de
> fencing, la recuperación a media sesión y el standby tibio. Un catálogo que declara lo
> retirado se lee como un plan, y `internal/obs.Catalog()` es esta lista. Lo que se fue
> vuelve con su mecanismo, desde git.

> **Que esta lista siga siendo `Catalog()` es un comando, no una afirmación** — se
> desincronizó dos veces, y la segunda le faltaban las tres series de la vista de lectura:
> ```
> for n in $(sed -n 's/^\t\t{"\([a-z0-9_]*\)".*/\1/p' internal/obs/metrics.go); do
>   grep -q "$n" arquitectura_mvp_volumenes_remotos_v5.md || echo "sin documentar: $n"
> done
> ```
> Vacío significa que el catálogo declarado está entero acá. Por eso los nombres se
> escriben completos abajo y no abreviados con llaves: una abreviatura hace que el chequeo
> ladre por algo que sí está, y un chequeo que ladra de más se ignora. Lo que el comando
> **no** dice es cuál tiene productor; eso está en `STATUS.md`, con su propia trampa
> anotada.

> **Segundo recorte, 2026-08-08: se fueron once series más, y la razón es distinta de
> la de ADR-0026.** Aquéllas nombraban mecanismos retirados; éstas nombraban mecanismos
> que **nunca existieron**, y estaban declaradas, instanciadas y grabadas por nadie —
> exportadas en cada scrape y permanentemente vacías. Eso es peor que una serie ausente:
> una serie vacía se lee como «esto no está pasando», no como «nadie está midiendo», y un
> dashboard montado sobre `s3_errors_total` mostraba un object store sano.
>
> Se fueron: `wal_oldest_unflushed_age_seconds` (el Log sabe *si* hay no-flusheados, no la
> edad del más viejo, y ningún Agent configura la cota a la que pertenece),
> `clock_offset_seconds` (nada lee chrony), `host_nvme_committed_ratio` (se deriva al
> colocar, no se graba), `clone_cross_host_total` (ADR-0026 borró el camino),
> `agent_memory_bytes` (el presupuesto de memoria de §10.1 nunca se construyó;
> `agent.Budget` es un presupuesto de *dispositivo*), `vhost_reconnects_total` e
> `inflight_recovered_total` (el incremento 3.3 no empezó) y las cuatro `s3_*` (§24 no
> existe como subsistema). Las que sí tenían mecanismo se **cablearon** en el mismo
> cambio, que es la otra mitad de la regla.
>
> Y lo que ahora lo sostiene no es este párrafo: es
> `TestEveryDeclaredMetricHasANonTestProducer` en `internal/obs`, que falla si una serie
> declarada no aparece en ningún `.go` que no sea un test. Se rechazó una lista de
> excepciones — las trece habrían estado en ella, puestas por quien declaró la serie, y la
> lista se leería como un plan igual que se leía el catálogo.

**WAL local**: `wal_append_latency_seconds` (alrededor del append, sobre el reloj
inyectado), `wal_fdatasync_latency_seconds` (alrededor del syscall, que bajo ADR-0026 **es**
el contrato de durabilidad, §14.8 — se graba también cuando falla, porque un dispositivo
cuyo fdatasync tarda lo bastante como para fallar es justo el caso para el que existe la
serie), `wal_unflushed_bytes`, `wal_local_sequence`, `wal_durable_sequence`,
`wal_out_of_space` (1 mientras el dispositivo rechaza appends por espacio, §5.7).

`wal_published_sequence` se retira con el checkpoint: en V1 nada publica, así que sería
una serie permanentemente en 0.

**Imagen y snapshots**: `image_publish_duration_seconds` (lo que tarda en salir del host
al parar: en V1 es el único momento en que algo sale, así que es el coste de una sesión),
`snapshot_publish_duration_seconds`, `snapshot_pause_duration_seconds` (~0 esperado; se
mide alrededor del *congelado*, no de la subida).

**Leases** (liveness, ya no durabilidad): `lease_remaining_seconds`,
`lease_renewal_failures_total`.

**Fleet**: `clone_same_host_total` (se graba donde se toma la decisión de placement, que es
el único lugar que conoce a la vez lo que se pidió y lo que se eligió), `chain_depth`,
`discarded_bytes_total` (se graba en el momento en que el número cambia, no lo lee un
poller).

**Vista de lectura** (agregadas en 2026-08-06; §10.1 pide que nada crezca sin cota y
`cow.IntervalMap` es la única estructura por volumen cuyo tamaño lo decide el guest y no la
configuración): `read_view_bytes`, `read_view_extents`, `read_view_layers`. Las tres las
graba el dueño del mapa bajo el lock que lo serializa, en el paso durable — no las muestrea
un poller, porque la estructura no es segura de leer en concurrencia y un poller sería una
segunda cosa pidiendo el lock del volumen en el data path.

### 26.3 Alertas mínimas

> **Ninguna de estas alertas existe como artefacto** — no hay reglas de Prometheus, no hay
> `deploy/`, y ninguna comparación contra un umbral vive en el código. La lista es lo que
> habría que escribir el día que haya un colector con reglas, y hasta entonces es una
> intención, no un mecanismo. Se dejó porque nombra los umbrales que alguien tendría que
> elegir; se marca porque un lector la contaba como observabilidad construida.
>
> Las tres que nombraban series retiradas se fueron con ellas
> (`wal_oldest_unflushed_age`, `clock_offset_seconds`, `host_nvme_committed_ratio`): una
> alerta sobre una serie que nadie graba no dispara nunca, y es la peor clase de alerta
> porque su silencio se lee como salud.

- `wal_unflushed_bytes > 80%` del límite.
- Cualquier `wal_out_of_space` en 1: el dispositivo está rechazando escrituras.
- `lease_renewal_failures_total` creciendo.
- Certificados mTLS a < 30 días.
- Cualquier error de checksum, GCM o divergencia (severidad máxima).
- `snapshot_pause_duration_seconds > 0` sostenido (regresión del diseño sin pausa).

---

## 27. Versionado de formatos y fleet mixto (nuevo)

Sin esta política, el primer cambio de formato con el fleet a medias actualizado produce un volumen ilegible.

1. **Read-old ilimitado**: un Agent vN+1 lee todo formato de su misma major (WAL, segmentos, checkpoints, manifests, descriptors).

   > **Hoy el código hace lo contrario, y es lo correcto para V1.** Los decodificadores
   > aceptan exactamente la versión actual y rechazan cualquier otra con `ErrBadVersion`, y
   > `cmd/volume-agent` declara `maxFormatVersion = 1`: hay **una** versión de formato, así
   > que «leer formatos viejos» no tiene objeto y aceptar una versión que este binario no
   > entiende sería aceptar bytes que no puede interpretar. Eso es lo que dice CLAUDE.md
   > sobre los formatos antes del primer despliegue: cambian en su lugar, sin v2 al lado de
   > v1. Esta regla —y con ella INV-19— **empieza a atar el día que existan dos versiones**,
   > que es exactamente cuando la compatibilidad empieza a costar algo real. Lo que hay que
   > recordar es que la lectura estricta es una decisión con fecha de vencimiento, no una
   > propiedad del diseño: el primer bump de formato tiene que traer el lado read-old con
   > él, o el fleet a medias actualizado produce el volumen ilegible que esta sección
   > existe para evitar.
2. **Write-new gated**: los formatos nuevos se activan por feature flag **después** de que todo el fleet puede leerlos (dos fases: desplegar lectores → habilitar escritores).
3. El CP registra `max_format_version` por host (tabla `hosts`) y **rechaza** operaciones incompatibles (ej. recuperar en un host viejo un volumen escrito con formato nuevo; placement de clones sobre hosts que no leen el formato del snapshot).

   > **La columna se registra de punta a punta; el rechazo no existe.** `placement` no lee
   > el campo, y con una sola versión de formato no tendría con qué comparar. Es la misma
   > fecha de vencimiento del punto 1.
4. Misma disciplina para el API gRPC del CP (compatibilidad hacia atrás dentro de la major; deprecaciones anunciadas).
5. Los headers ya llevan `Version` + magic distintos por tipo; los campos `Reserved` existen para extensiones sin bump de versión.

   > **Era cierto sólo de la mitad que no lo necesitaba, y se corrigió el 2026-08-09.**
   > Los formatos binarios del WAL sí llevaban `Version` y magic y rechazaban cualquier otra
   > con `ErrBadVersion` — y son **locales**: viven una sesión y los lee el mismo binario que
   > los escribió. Los objetos de S3 —`manifest.json`, los manifiestos de snapshot y
   > `descriptor.json`, que son los únicos que cruzan hosts y sobreviven a una sesión— **no
   > llevaban versión ninguna**, y son JSON: `json.Unmarshal` descarta en silencio los campos
   > que no conoce. O sea que la estrictez estaba exactamente al revés, y el modo de fallo del
   > lado que importa no era un rechazo sino su ausencia: un Agent leyendo un manifiesto de un
   > formato más nuevo lo decodifica limpio, tira lo que no entiende y sirve el volumen.
   >
   > Los tres llevan ahora `format_version` (`framed.FormatVersion`, un número para los tres
   > porque en este árbol cambian juntos) y todo lector rechaza lo que no sea exactamente el
   > suyo, con **dos errores distintos** —`ErrFormatTooNew` y `ErrFormatTooOld`— porque lo que
   > el operador tiene que hacer difiere y no se deduce del objeto: *más nuevo* es «este host
   > está atrasado, avanzalo, no toques el objeto», *más viejo* es el caso que se vuelve
   > trabajo de read-old el día que exista. Un objeto sin el campo se rechaza como más viejo,
   > no se asume generación 1.
   >
   > **Esto no es read-old y no es una capa de compatibilidad** —CLAUDE.md prohíbe
   > construirlas antes de que algo las exija—: es la capacidad de *detectar* que la
   > compatibilidad se rompió. Se agrega ahora por la razón por la que §15 reservó los campos
   > de cripto el día 1: agregar un campo obligatorio a objetos que ya viven en un bucket que
   > alguien va a releer no es un cambio que se pueda hacer.

---

## 28. Operación de fleet (nuevo)

### 28.1 Cordon / Drain

- `cordon <host>`: no colocar nada nuevo (estado en tabla `hosts`).
- `drain <host>`: operación reconciliada de larga duración; mueve volúmenes fuera (snapshot + restore cross-host en el MVP, priorizando destinos con caché/standby), con progreso visible (`operations`) y cancelable. Prerequisito de todo mantenimiento de kernel/NVMe/decomiso.

> **El drain se borró y el cordon dejó de ser un verbo humano; este párrafo describía las
> dos cosas al revés hasta 2026-08-07.** `controlplane/drain.go` no existe y nada mueve un
> volumen de host: `HostDraining` sobrevive como estado del ciclo de vida sin productor, y
> el movimiento entre hosts es dos pasos manuales — `-detach-volume`, observar que el host
> paró y publicó, `-attach-volume` (§7).
>
> El cordon, en cambio, **lo aplica el Control Plane solo**, sobre la medición de
> dispositivo que el heartbeat del Agent reporta (ADR-0013 §3, `internal/cpserver/pressure.go`).
> Y no es un umbral sino una **banda de Schmitt**: cordona al 70% usado
> (`cpserver.DefaultBand().Cordon`) y vuelve a ACTIVE solo por debajo del 65%
> (`.Uncordon`), ambos ajustables con `-cordon-used-ratio` / `-uncordon-used-ratio`. Un umbral solo es un cordon que oscila — un host parado sobre la
> línea la cruza en los dos sentidos en heartbeats consecutivos, y cada cruce es una
> escritura, un cambio de estado que lee toda decisión de placement de la flota, y una
> línea en lo que sea que un operador esté mirando. Volver exige liberar un 5% del
> dispositivo, que no es jitter. Se rechazó además un *dwell* ("seguir cordonado hasta N
> heartbeats sin presión"): necesita estado que esto no tiene — cuándo se despejó la
> presión, por host — o sea una columna escrita en el RPC más frecuente que hay, o memoria
> del CP que un cambio de líder descarta, con lo que una flota en failover dejaría hosts
> cordonados indefinidamente.
>
> Como ahora `state = 'CORDONED'` puede haberlo puesto un humano o el bucle, la tabla
> `hosts` guarda **por qué** (`lifecycle.CordonReason`: `OPERATOR` o `DEVICE_PRESSURE`), y
> el escritor automático solo puede pisar `''` o `DEVICE_PRESSURE` — un cordon puesto por
> una persona, por una causa que la flota no puede ver, no lo levanta la presión al bajar.
>
> Lo que §28.2 dice sigue en pie y es lo que hace el `placement` de V1: la admisión mira
> lo que un host **está usando**, no solo lo que le prometieron.

### 28.2 Capacidad y placement

Heartbeat del Agent reporta NVMe total/usado/comprometido. El CP aplica la política de sobresuscripción declarada al decidir placement, y la alerta `host_nvme_committed_ratio` avisa antes del incidente.

> **Esto es lo que hace `internal/placement`, con una corrección: el heartbeat no reporta lo
> comprometido.** Reporta total y usado; lo comprometido se deriva sumando los volúmenes que
> el catálogo ya coloca en el host (§8). Y son **dos** techos, no uno: `Policy.Admits` exige
> que lo comprometido más el volumen nuevo entre en `MaxOversubscription` **y** que lo
> *medido como usado* esté bajo `MaxUsedRatio` (`DefaultMaxUsedRatio` = 0,85, el mismo
> número que el `GuestRatio` del Agent en §5.7, deliberadamente escrito igual). Colapsarlos
> en un solo techo sobre `max(comprometido, usado)` hace que el más bajo esconda al otro:
> son dos preguntas distintas —cuánto se prometió y cuánto hay— y una sola respuesta oculta
> la que no disparó.

### 28.3 Deploys

- Agent: rolling restart transparente (reconexión vhost-user). Regla: nunca desplegar un writer de formato nuevo antes de completar la fase de lectores (§27).
- CP: standby toma liderazgo por término; las operaciones en curso re-convergen por reconciliación.
- Backend de objetos: rolling según su propio procedimiento; la suite de conformidad (§6.1) corre contra la versión nueva antes.

### 28.4 Runbooks exigidos

Failover de writer, restore de PG (PITR + rebuild-metadata), pérdida de nodo del object store, drain, break-glass del CP, rotación de KEK, respuesta a alerta de divergencia/checksum. Cada uno con tiempos medidos, no estimados.

---

## 29. Debilidades residuales y mitigaciones explícitas

Actualizado: se eliminan las resueltas por diseño en v5 y se agregan las nuevas que v5 introduce.

> **Cuatro de las ocho se fueron con ADR-0026 (2026-08-02), no se resolvieron.** Son
> debilidades *de la cadena de durabilidad remota* y vuelven con ella: la **1** (el FLUSH
> ya no paga red ni PUT — el ACK es `fdatasync`, §14.8), la **2** (ningún ACK depende del
> lease, así que PostgreSQL caído no degrada durabilidad), la **3** (nada promociona) y la
> **8** (no hay objectización ni truncado a media sesión). La **4** cambia de forma: un
> clon en otro host paga la descarga completa, y sin standby tibio ni lazy loading la
> mitigación que queda es arrancar el clon en el host de origen (§20). **5, 6 y 7 siguen
> en pie**, y lo que queda abierto de cada una vive en `docs/plan/STATUS.md`.

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

> **Ninguna de las tres existe, y la que el banner de arriba ofrecía en su lugar —arrancar
> el clon en el host de origen— tampoco mitiga nada todavía (§20).** La debilidad está sin
> mitigar: un clon paga la descarga completa de su ancestría, en cualquier host. Tampoco
> hay nada que mida el RTO frío, así que no se puede publicar (criterio 7 de §31).

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

> **Esta lista es el plan de v5, no la cola de trabajo, y no se puede leer como un
> progreso.** Cinco de sus catorce puntos describen mecanismos que ADR-0026 retiró: el **6**
> (WAL remoto, batching, summary objects), el **10** (objectización, checkpoints, GC), el
> **12** (standby tibio, compactación, aplanado), la mitad de fencing del **7** y la mitad
> de "recovery con S3 como autoridad" del **8** — cuyo `rebuild-metadata` sí existe (§22.5).
> El **9** pierde el resize (V2, §3) y el **11** pierde el drain y el cross-host (§28.1).
> **Dónde está el estado de verdad:** `docs/plan/STATUS.md`, que es el único archivo que
> lleva estado en este repositorio y que recorre estas fases una por una diciendo qué
> símbolo la sostiene.

1. **Esqueleto con interfaces simulables** (reloj/red/disco/S3) + harness DST mínimo + tracing/logging estructurado. *Nada de `time.Now()` directo desde el primer commit.*
2. Layout local + tres dispositivos + OverlayFS.
3. vhost-user-blk con backend raw + **reconexión + inflight shmfd**.
4. CoW (segmentos 64 KiB) + WAL local con **extents reales** + formato v2 con campos de cifrado + property tests del WAL.
5. **Cifrado por volumen** (DEK/KEK, KMS de desarrollo) + DISCARD/WRITE_ZEROES.
6. WAL remoto: batching bajo demanda + idempotencia de PUT + summary objects.
7. PostgreSQL + Control Plane con **término verificado** + modelo de reconciliación + **leases y protocolo de fencing completo** (§12) bajo DST con particiones y deriva de reloj.
8. Recovery con **S3 como autoridad** + recovery-point + `rebuild-metadata` básico.
9. Snapshots sin pausa + clones same-host + ~~resize (grow)~~ (V2, §3).
10. Objectization + checkpoints + GC mark-and-sweep (buckets con versioning + Object Lock desde el primer entorno de staging).
11. Cross-host por materialización completa + cordon/drain + contabilidad de capacidad.
12. Standby tibio + compactación de WAL objects + aplanado de cadenas.
13. Endurecimiento: fault injection sobre hardware real, suite de conformidad de backends, runbooks con tiempos medidos.
14. Post-MVP: lazy loading (overlaybd-style), multi-queue, io_uring, flush selectivo FUA, QoS entre tenants, renovación de lease vía S3.

---

## 31. Criterios de éxito del MVP

> **Cuatro criterios son de V2 desde ADR-0026 (2026-08-02)** y no se pueden cumplir ni
> fallar en V1, porque su sujeto no existe: el **4** (ningún ACK con lease vencido), el
> **8** (stale writer incapaz de confirmar durabilidad tras `lease_ttl`), el **10**
> (recovery cuyo punto durable se determina desde S3) y el **14** (WAL no eliminado antes
> de durabilidad remota verificada; GC). El **7** conserva el clon cross-host y pierde el
> standby tibio; el **15** nombra `wal_objects`, que ya no existen, y habrá que decir
> sobre qué se mide antes de poder cumplirlo. El resto sigue siendo el criterio de V1, y
> el **3** —que decía en presente el contrato retirado— está reescrito.
>
> **Dos más, agregados 2026-08-06 y 2026-08-07.** El **16** (resize grow end-to-end) es V2
> con el objetivo 14 de §3 — DEV-0023, resuelto ahí. Y el **17** (`rebuild-metadata`
> reconstruye PG desde S3) **es el único criterio de esta lista que ya se cumple en un
> entorno de prueba**: `controlplane.RebuildMetadata` lo hace desde dos objetos por volumen
> y su arma DST es `a-rebuilt-catalog-can-serve-its-volumes`, que no comprueba que
> volvieron las filas sino que un Agent nuevo sirve los bytes que el guest original
> escribió (INV-20). Lo que no vuelve es el placement, porque ningún objeto lo registra.

1. Boot con los tres dispositivos. — **fuera de alcance de `storage` (§9);** este módulo
   demuestra el suyo, que es el persistente, en el lane de guest real.
2. Docker/containerd solo en efímero. — **fuera de alcance (§9).**
3. Un FLUSH ACKeado sobrevive a la caída del proceso, del Agent y de QEMU; el volumen entero llega al object store al parar y un arranque posterior lo lee de vuelta. **La pérdida del host pierde la sesión** (RPO de una sesión, §2, ADR-0026). Demostrado por el harness DST con checkers de invariantes y por el lane de guest real.
4. Ningún ACK de durabilidad emitido con lease vencido (invariante DST).
5. Snapshot portable, crash-consistent, con `snapshot_pause_duration ≈ 0`.
6. Clone same-host sin transferencia significativa. — **no se cumple:** crear el clon no
   transfiere nada, servirlo baja la ancestría entera (§20).
7. Clone cross-host desde S3; RTO frío medido y publicado; RTO con standby tibio en minutos.
8. Stale writer incapaz de confirmar durabilidad tras `lease_ttl` (verificado con partición + acceso a S3 intacto).
9. PUT idempotente (incluyendo respuesta perdida).
10. Recovery cuyo punto durable se determina desde S3 y coincide con todo lo ACKeado.
11. Crash/deploy del Agent sin reinicio de VMs (inflight recuperado, cero I/O perdido o duplicado). — **sin mecanismo: incremento 3.3 no empezado** (§2).
12. Todo dato de VM fuera del host cifrado; borrado de volumen = crypto-shred.
13. Backpressure estricto antes de llenar NVMe.
14. WAL nunca eliminado antes de durabilidad remota verificada; GC incapaz de borrado permanente directo.
15. DISCARD reduce `wal_objects`/segmentos: el storage converge al working set en el test de churn.
16. ~~Resize (grow) online end-to-end.~~ — V2 (§3).
17. `rebuild-metadata` reconstruye PG desde S3 en un entorno de prueba.
18. Fleet mixto vN/vN+1 opera sin volúmenes ilegibles (test de upgrade en CI).
19. Métricas y trazas mínimas visibles desde el día 1; runbooks con tiempos medidos.

---

## 32. Conclusión

> **Es el resumen del sistema completo, y su mitad remota está retirada (ADR-0026,
> 2026-08-02).** En V1: el lease manager no gobierna ningún ACK (es liveness, §14.8); no
> hay objectizer, compactador ni standby hidrator; el object store no guarda WAL objects,
> checkpoints, summary ni recovery-points, y no es la autoridad de ningún punto durable —
> guarda **una imagen por volumen y sus snapshots**, escritos al parar; y el fencing es un
> compare-and-set sobre el manifiesto en ese momento (§12), no una ventana cerrada por
> leases. Sigue exacto todo lo demás: los tres dispositivos, el CoW, el cifrado de todo lo
> que sale del host, el WAL local, y que **no existe replicación host-to-host**.
>
> **En el diagrama de abajo**, la línea del Control Plane nombra además tres verbos que no
> existen: `resize` es V2 (§3, DEV-0023), `drain` se borró (§28.1) y el GC tampoco marca
> (§21). Lo que sí falta en el diagrama y sí existe es lo de §28.1: el CP **cordona hosts
> por sí solo** según la presión del dispositivo que reporta el heartbeat.

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
FLUSH / FUA    → fdatasync local → durable_sequence → ACK      (§14.8)
STOP           → la imagen del volumen sale al object store, CAS del manifiesto
SNAPSHOT       → capturar N → publicar en background → pausa ≈ 0
```

No existe replicación host-to-host. La durabilidad vive en S3 y su límite es la durabilidad del backend, que ahora es un requisito explícito, no un supuesto. El fencing ya no tiene ventana: un writer particionado no puede confirmar durabilidad pasado su lease, y el nuevo writer solo arranca después de esa garantía. La verdad en recovery se lee de S3, y PostgreSQL entero es reconstruible. Todo lo que sale del host va cifrado. El sistema está diseñado para el día 2: deploys sin reiniciar VMs, GC incapaz de causar el peor incidente, drains reconciliados, RTOs medidos, y un contrato de durabilidad demostrado millones de veces por noche en simulación determinística — no solo creído.

Esta v5 es la base recomendada para empezar a codificar, comenzando por la fase 1 del roadmap: las interfaces simulables no son opcionales ni postergables.

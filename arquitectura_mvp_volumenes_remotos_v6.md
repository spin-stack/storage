# Arquitectura MVP de volúmenes remotos (v6) — qcow2 + commits inmutables

**Reemplaza a v5.1 (2026-08-11).** v5 describía un storage engine propio: `vhost-user-blk`,
CoW por bloques, WAL local, WAL remoto, dirty maps, objectization, checkpoints, replay y
recovery por secuencias. Esa arquitectura era válida conceptualmente y tenía demasiadas
responsabilidades para el objetivo. La versión anterior está en `git log`.

> **QEMU administra el formato CoW local mediante qcow2. Este sistema administra
> únicamente commits inmutables, publicación en object storage y recuperación.**

No construimos un storage engine propio mientras QEMU resuelva esa parte de forma
suficientemente correcta para el MVP.

---

## 1. Objetivos

1. Ejecutar una VM sobre almacenamiento local de alto rendimiento.
2. Mantener un disco persistente basado en qcow2.
3. Crear commits consistentes del estado del volumen.
4. Publicar esos commits en S3 o un backend S3-compatible.
5. Recuperar un volumen en otro host usando únicamente object storage y metadata.
6. Crear snapshots y clones.
7. Mantener single-writer con fencing.
8. Evitar cualquier comunicación directa entre hosts.
9. Mantener la implementación suficientemente chica como para probar exhaustivamente
   recovery y fallos.
10. Poder evolucionar hacia fragmentación o dirty bitmaps sin cambiar el modelo de
    consistencia.

## 2. No objetivos

`vhost-user-blk` propio · NBD como data path · CoW propio por bloques · WAL por bloques ·
WAL remoto · replay de WAL · dirty block map propio · dirty bitmaps de QEMU ·
objectization · multi-writer · live migration · replicación host-to-host · Raft ·
streaming entre nodos · deduplicación global · compresión · cifrado a nivel de fragmento ·
lazy loading · `io_uring` · parser qcow2 propio · GC sofisticado · compactación incremental
sofisticada.

Si algo de esto existe parcialmente, no se expande salvo para migrar datos o completar una
prueba.

---

## 3. El cambio de semántica

v5 pretendía:

```text
WRITE  → WAL local → ACK
FLUSH  → persistencia local → publicación remota → ACK
```

Un FLUSH exitoso era una frontera de durabilidad frente a la pérdida del host. **Esa
semántica queda eliminada.**

```text
WRITE  → QEMU → qcow2 local → ACK
FLUSH  → fsync/fdatasync local → ACK
COMMIT → durabilidad remota
```

S3 no participa en el data path.

> **FLUSH/FUA garantiza durabilidad local. COMMIT garantiza durabilidad remota.**
>
> **El RPO frente a la pérdida completa de un host es el tiempo transcurrido desde el
> último commit publicado.**

Esa propiedad debe estar explícita en código, tests y APIs.

---

## 4. Arquitectura

```text
                         ┌────────────────────┐
                         │    PostgreSQL      │
                         │ metadata, ownership│
                         │ epoch, commits     │
                         └─────────┬──────────┘
                            control plane
                                   ▼
┌────────────────────── Compute Host ──────────────────────┐
│   QEMU ──direct file I/O──▶ active.qcow2                 │
│     │                            │                       │
│     └── QMP external snapshot ──▶ sealed.qcow2           │
│                                   │                      │
│                                 Agent                    │
└───────────────────────────────────┼──────────────────────┘
                                    │ S3 API
                                    ▼
                        ┌──────────────────┐
                        │  Object Storage  │
                        │ immutable layers │
                        │ immutable commits│
                        │ mutable HEAD     │
                        └──────────────────┘

Host A ─────X───── Host B     (no existe)
```

La recuperación cross-host ocurre exclusivamente mediante object storage.

### Componentes

**QEMU** lee y escribe el volumen activo, mantiene la semántica qcow2, ejecuta los flushes
del guest, crea external snapshots y continúa escribiendo en el tip nuevo. Escribe
directamente sobre archivos locales. No se pone otro storage engine en el data path.

**Volume Agent** deja de ser un block storage engine: prepara filesystem y directorios,
crea y abre chains qcow2, controla QEMU por QMP, coordina commits, sella layers, calcula
hashes, sube y baja objetos, reconstruye chains, coordina snapshots y clones, hace
recovery, valida epoch, GC básico y métricas. Queda fuera del I/O normal de la VM.

**PostgreSQL** sigue siendo la autoridad de control: `volume_id`, `current_epoch`,
`writer_host`, `state`, `head_commit_id`, `head_etag`, timestamps. No almacena qcow2, ni
bloques, ni WAL, y no está en el data path.

**Object storage** es la frontera portable: `volumes/`, `commits/`, `layers/`. Los objetos
de datos son inmutables; sólo `HEAD` es mutable, mediante compare-and-swap.

---

## 5. Layout local

```text
/var/lib/volume-agent/volumes/<volume-id>/
├── layers/<layer-id>.qcow2   # todos los tips que tuvo, tip actual incluido
├── active/current            # una línea: el path absoluto del tip
├── cache/
└── state.json
```

El archivo que QEMU está usando nunca se procesa con herramientas offline que puedan
modificarlo.

**Un archivo de layer no se renombra, no se reusa y nunca significa dos cosas** — corregido
2026-08-23, contra QEMU real. El árbol original tenía `active/current.qcow2` fijo y los
sellados mudándose a `sealed/<id>.qcow2`; rotar eso significa que un path —el que QEMU
recibió al arrancar— pasa a nombrar otro archivo, y medirlo mostró dos costos: QEMU recuerda
para siempre el *string* con el que abrió un nodo, así que el layer sellado sigue llamándose
`active/current.qcow2`, que ya nombra el tip vivo; y `query-block` deja de contestar con un
path, devuelve `json:{...}`, que es exactamente el string contra el que el Agent compara para
saber si el archivo abierto es suyo. Con ids en el nombre los dos problemas no existen en vez
de estar manejados. `active/current` no es la imagen: es un **puntero** a ella, y es el
contrato con quien lanza la VM.

## 6. Cadena qcow2

```text
base.qcow2 → tip-001 (SEALED) → tip-002 (SEALED) → tip-003 (ACTIVE)
```

Cada commit corresponde, inicialmente, a una nueva capa.

## 7. QMP

Obligatorio. El Agent lo usa para pedir flush de los block devices, crear el external
snapshot, cambiar QEMU al nuevo active tip y confirmar que lo está usando.

**No ejecutar `qemu-img` contra el archivo que QEMU tiene abierto para escritura.**
`qemu-img` sólo sobre layers offline o sellados. Permitido: `create`, `info`, `check`,
`convert`, `rebase`. No se escribe un parser qcow2 propio.

## 8. Guest freeze

No es parte de la consistencia básica. Sin freeze, un commit representa un estado
equivalente a recuperar una máquina después de un crash, y eso alcanza para el MVP.
Opcionalmente `guest-fsfreeze-freeze` / `thaw` alrededor del commit para consistencia de
filesystem. La consistencia application-aware queda fuera de alcance.

---

## 9. Protocolo de commit

Entrada mínima: `volume_id`, `epoch`, `expected_parent_commit`, `request_id`.

```text
 1. validar writer y epoch
 2. capturar HEAD actual
 3. pedir flush a QEMU
 4. external snapshot por QMP
 5. el active pasa a SEALED
 6. crear nuevo active
 7. confirmar que QEMU escribe sobre el tip nuevo
 8. reanudar operación normal
 9. metadata y SHA-256 del layer sellado
10. sellar el layer con la DEK del volumen  (§10)
11. subir el layer
12. publicar el manifest inmutable del commit
13. CAS de HEAD
14. registrar el commit publicado en PostgreSQL
15. marcar archivos locales elegibles para limpieza
```

**La VM no permanece pausada mientras se sube el layer.** El cambio de tip es breve; la
transferencia ocurre después de rotar.

Orden de publicación, siempre:

```text
PUT layer → PUT commit manifest → CAS HEAD
```

Nunca al revés. Un commit sólo existe contractualmente cuando `HEAD` apunta a él.

---

## 10. Cifrado — DECIDIDO 2026-08-11

**El layer se sella al subir, con la DEK del volumen.** No se usa el cifrado de qcow2.

La razón es independencia: el cifrado sigue siendo nuestro y no de QEMU, y con eso
sobreviven sin cambios `crypto.KMS`, `descriptor.DEKWrapped`, el `kek_id` del catálogo y el
crypto-shred — borrar un volumen sigue siendo destruir su DEK envuelta. Delegar en qcow2
LUKS habría movido el manejo de claves adentro de QEMU, que es exactamente la
responsabilidad que este rediseño saca de nuestro lado en el data path y no queremos
devolverle en el lado de las claves.

Lo que esto **no** da, y hay que decirlo: el archivo qcow2 **local** queda en claro. En v5
el WAL local estaba cifrado. La frontera pasa a ser el host: lo que sale va sellado, lo que
está en el disco local no. Un host comprometido ve los datos del guest — lo cual ya era
cierto para la memoria de la VM.

`HEAD` y los manifests son metadata estructural y van en claro, para que
`rebuild-metadata` funcione sin material de claves (igual que en v5 §15.3).

---

## 11. Quién dispara Commit — DECIDIDO 2026-08-11

La política de disparo **es** la política de RPO: `last_successful_commit_age` no es una
métrica del sistema, es el producto.

**El Control Plane dice cuánto; el Agent decide cuándo.** `DesiredVolume` lleva un
`rpo_target` por volumen y el Agent commitea solo.

La división es así porque el Control Plane no sabe cuánto escribió el guest, y un commit
disparado por reloj sin mirar eso sube un layer vacío. El Agent sabe las dos cosas. Y el
objetivo es por volumen porque es una promesa del catálogo: un runner de CI tolera 30
minutos y un volumen con una base de datos quiere 60 segundos.

Tres disparadores, el primero que ocurra:

1. **Edad** — pasó el `rpo_target` desde el último commit que cubre todo lo escrito.
2. **Tamaño** — el tip activo pasó un umbral. Acota el layer y, sobre todo, el tiempo de
   recovery: lo que se descarga es exactamente esta cadena.
3. **Explícito** — snapshot, detach, drain o un operador, por el mismo camino de código.

### Dos invariantes

**Un volumen sin escrituras no genera un commit.** Si nada cambió, el disparador de edad no
rota. Sin esto, un volumen ocioso sube un layer vacío cada `rpo_target` para siempre, y
`last_successful_commit_age` deja de significar lo que dice: la definición correcta es *la
edad del commit más nuevo que cubre todo lo escrito*, de modo que un volumen que no escribió
está **en su RPO** y no atrasado.

**No rotar mientras haya un layer sellado sin publicar.** Con S3 caído, seguir rotando por
reloj produce una cadena de layers chiquitos, cada uno un commit que nunca aterrizó. Rotar
sólo cuando el sellado anterior ya publicó deja un tip activo que crece localmente y como
mucho un sellado pendiente, y acota la profundidad local durante toda la caída. También
resuelve el crash de §15: al reiniciar, si hay un sellado sin publicar se publica **ése**, no
se rota otra vez.

### Lo que no está resuelto

Con QEMU en el data path **no podemos rechazar una escritura del guest**. Si S3 no vuelve, el
qcow2 llena el filesystem y QEMU devuelve ENOSPC, que el guest ve como error de I/O — pero no
lo elegimos nosotros y no lo anticipamos. Lo que sí se puede: alarmar sobre
`unpublished_local_bytes` mucho antes, y negar attaches nuevos en ese host. Si en algún
umbral el Agent debe **parar la VM** es una decisión de producto que el MVP no toma, y el
default silencioso —que se llene el disco— queda escrito acá para que sea una elección y no
un olvido.

Los defaults de `rpo_target` y del umbral de tamaño **se fijan midiendo**, no eligiendo: la
pausa del external snapshot pone el piso del primero y el throughput de subida contra el
objetivo de recovery pone el segundo. Es el criterio de salida de la Etapa 2.

### Lo medido (2026-08-23, `task demo:stage2`)

Un guest Linux real escribiendo sobre NVMe, umbral de 4 MiB, ciclo de reconciliación de
300 ms, tres rotaciones bajo carga:

- **Pausa del external snapshot: 1.11 / 1.46 / 1.53 ms** (min / mediana / max). No es el
  piso de `rpo_target`: a este costo, rotar cada pocos segundos es gratis para el guest, y
  lo que fija el default es el throughput de subida — que no se puede medir hasta la
  Etapa 3, porque todavía nada sube.
- **El umbral de tamaño es un piso, no una cota.** Los layers sellados salieron de 32 MiB
  con el umbral en 4: el tip se mide una vez por ciclo, así que un layer pesa el umbral
  más lo que el guest escribió desde la última mirada. Con QEMU en el data path no
  podemos rechazar esa escritura, así que **no hay forma de acotar el tamaño de un
  layer**; sólo se elige cada cuánto se mira. Quien dimensione uploads debe planificar
  umbral + un ciclo del guest más rápido que vaya a alojar.

---

## 12. Formatos

**Commit manifest** (inmutable):

```json
{
  "format_version": 1,
  "volume_id": "vol-123",
  "commit_id": "01J...",
  "parent_commit_id": "01H...",
  "epoch": 17,
  "virtual_size": 53687091200,
  "layer": { "object_key": "layers/sha256/ab/cd/...", "size": 427819008, "sha256": "abcd...",
             "frame_bytes": 65536, "layer_id": "01J..." }
}
```

Sin `created_at`: el `commit_id` es un UUID v7 (INV-22), o sea *ya es* un timestamp de
milisegundos, y por eso los ids son v7. Un segundo timestamp tendría que salir de un reloj de
pared que el Agent no tiene (INV-01 le da un instante monotónico) y sería un campo que puede
contradecir al id que tiene al lado.

El `sha256` es sobre el objeto **tal como está guardado** —sellado— porque esa es la mitad de
la integridad que un recovery puede verificar sin material de claves, que es lo que §10 le
pide a `rebuild-metadata`. El tag GCM de cada trama es la otra mitad. `frame_bytes` viaja en
el manifest en vez de estar fijo en el formato, así que cambiarlo no es una migración: un
layer viejo se abre con el número que trae. El default es 64 KiB —el cluster de qcow2—
medido: 64 KiB, 256 KiB y 1 MiB sellan todos a 7.7 GiB/s y 4 MiB es más lento porque la trama
deja de entrar en caché; el overhead a 64 KiB es 0.043%.

**HEAD** (mutable, CAS):

```json
{ "version": 1, "volume_id": "vol-123", "commit_id": "01J..." }
```

`If-Match: <etag>` para actualizar, `If-None-Match: *` para crear. El ETag es un token
opaco de concurrencia y **no** un checksum: la integridad de layers y manifests usa SHA-256
independiente. Los dos objetos llevan la línea de digest y el `format_version` de
`internal/framed`, por la razón de siempre — un bit dado vuelta en `HEAD` es el volumen
entero.

---

## 13. Single writer y fencing

El CAS sobre `HEAD` **no alcanza**: dos hosts pueden escribir diez minutos cada uno y tener
histories localmente válidos aunque sólo uno pueda publicar. Por eso se conserva el `epoch`.

Cada attach que obtiene ownership recibe `epoch = anterior + 1`, y toda operación
administrativa mutable lo incluye. Un Agent cuyo epoch dejó de ser actual no puede publicar
commits, ni cambiar HEAD, ni crear snapshots publicables, ni modificar metadata.

**El epoch es un fencing token, no metadata informativa.**

## 14. Recovery

**Mismo host:** consultar ownership y epoch, inspeccionar el estado local, validar la chain,
identificar el active tip, reiniciar QEMU. No se vuelve a descargar lo que ya está validado
localmente.

**Pérdida del host:** marcar `RECOVERY_REQUIRED`, confirmar que el writer anterior está
fenced, incrementar epoch, elegir host, leer HEAD, obtener el manifest, recorrer
`parent_commit_id`, descargar los layers necesarios, validar SHA-256, reconstruir la chain,
crear un active tip nuevo, iniciar QEMU, pasar a ACTIVE.

Se recupera exactamente hasta el último commit publicado.

## 15. Fallos

| | |
|---|---|
| Crash antes del external snapshot | no hay commit nuevo; el active sigue siendo la fuente local |
| Crash después del snapshot | puede haber un sellado y un active nuevo sin commit publicado; el recovery local lo detecta y publica el sellado (§11) |
| Layer subido, manifest no publicado | objeto huérfano; GC |
| Manifest publicado, HEAD viejo | commit no publicado; GC |
| CAS exitoso, respuesta perdida | leer HEAD; si apunta al commit pedido, fue exitoso |
| CAS falla | otro actor movió HEAD. **No sobrescribir.** Con single-writer correcto esto es un problema de fencing, un recovery concurrente o un bug |
| S3 no disponible | la VM sigue; los commits no terminan; backpressure operativo antes de llenar el disco |
| PostgreSQL no disponible | I/O local sigue; no hay attach, ownership, recovery cross-host ni cambio de epoch |

## 16. Estados

```text
DETACHED · ATTACHING · ACTIVE · COMMITTING · RECOVERY_REQUIRED · RECOVERING · DETACHING · ERROR
```

`COMMITTING` no implica que la VM esté detenida: una vez rotado el tip, sigue escribiendo
mientras se publica el commit anterior.

## 17. Idempotencia

Toda operación del control plane acepta `request_id`. Crear un commit es idempotente: ante
una respuesta perdida, el reintento debe determinar si el commit no empezó, está en
progreso, el manifest existe, HEAD ya apunta a él, o la publicación necesita reconciliación.
**Nunca crear histories distintos por repetir el mismo `request_id`.**

## 18. Snapshots y clones

Un snapshot es una referencia inmutable a un commit (`snapshot_id`, `volume_id`,
`commit_id`, `created_at`). Crearlo es commitear y registrar la referencia; si ya hay un
commit suficientemente reciente y no hace falta capturar writes nuevos, alcanza con
referenciarlo.

Un clone nace de un commit y **nunca modifica los layers del padre**. Same-host reutiliza
los archivos locales; cross-host descarga la chain, la verifica y crea el active child.

## 19. Fragmentación y compactación

**No fragmentar qcow2 salvo que las mediciones lo exijan.** Inicialmente
`layers/<sha256>.qcow2`, subido con multipart cuando corresponda. No se introduce CAS de
chunks por anticipar una optimización. Si los layers salen demasiado grandes, se evoluciona
a `chunks/<sha256>` y el manifest del layer pasa a listar chunks — sin alterar el protocolo
de commits ni la semántica de HEAD.

Las chains no pueden crecer sin límite. Política simple y configurable, ajustada por
medición (por ejemplo `layer_count >= 32` o `total_incremental_size >= 20 GiB`):
`qemu-img convert` offline sobre los archivos involucrados, publicando el resultado como
una raíz inmutable nueva. Sin compactación incremental sofisticada.

## 20. Garbage collection

Conservador. **Nunca borrar un objeto sólo porque no aparece en el HEAD actual**: los
commits forman un grafo por `parent_commit_id` y los snapshots mantienen commits viejos
vivos.

```text
roots = HEADs actuales + snapshots
```

Recorrer parents para alcanzabilidad; lo inalcanzable pasa a candidato, espera un grace
period y recién ahí se borra. El grace period tiene que ser lo bastante grande como para no
alcanzar operaciones en curso ni reconciliaciones.

## 21. Observabilidad

Por volumen: `current_epoch`, `active_commit`, `commit_duration`,
`external_snapshot_duration`, `layer_size`, `layer_upload_duration`, `layer_upload_bytes`,
`commit_publish_latency`, `cas_failures`, `chain_depth`, `local_disk_bytes`,
`unpublished_local_bytes`, `last_successful_commit_age`, `recovery_duration`,
`recovery_download_bytes`.

`last_successful_commit_age` es el más importante: **es el RPO potencial frente a la pérdida
del host**, medido.

## 22. Fault injection

Antes de optimizar rendimiento tiene que haber tests que maten procesos en cada frontera:
antes/durante/después del snapshot QMP, durante el hash, durante y después del upload,
después del manifest, durante y después del CAS antes de responder; timeout y caída de S3;
PostgreSQL caído; respuesta perdida; reboot; pérdida del host; epoch stale; disco lleno;
qcow2 corrupto; manifest corrupto; checksum incorrecto.

Para cada uno hay que poder responder: qué commit es visible, qué estado local queda, qué
objetos remotos quedan, si se recupera solo, y si puede perderse un commit confirmado.

> **Un commit que devolvió SUCCESS nunca puede desaparecer.**

## 23. Etapas

1. **qcow2 local** — creación, attach, restart, detach, persistencia local. Nada remoto.
2. **Rotación por QMP** — active → external snapshot → sealed + nuevo active, con la VM
   escribiendo. **Criterio de salida: la pausa del snapshot y el throughput de subida,
   medidos**, que son los que fijan los defaults de §11.
3. **Commit remoto** — sellar, SHA-256, subir, manifest, CAS de HEAD.
4. **Recovery** — reconstruir sólo desde PostgreSQL + S3.
5. **Snapshots y clones.**
6. **Fault injection** — todas las fronteras del protocolo. No se optimiza mientras el
   recovery no sea determinístico.
7. **Compactación** — límites de profundidad de chain.
8. **Optimización basada en medición** — recién acá: chunking, dirty bitmaps, compresión,
   prefetch, lazy loading, deduplicación, descarga paralela.

## 24. Criterios de éxito

1. Una VM hace I/O directo sobre qcow2 local.
2. Un restart del proceso conserva el volumen local.
3. Un commit rota el tip sin detener la VM durante la transferencia.
4. Un commit exitoso se reconstruye sin el host original.
5. La pérdida del host pierde los writes posteriores al último commit **y sólo ésos**.
6. Un stale writer no puede publicar.
7. Un CAS conflictivo nunca sobrescribe otro history.
8. Un retry de commit es idempotente.
9. Un snapshot es inmutable.
10. Un clone no modifica a su padre.
11. Same-host clone reutiliza información local.
12. Cross-host clone funciona sólo con object storage.
13. Un crash en cualquier frontera no corrompe un commit confirmado.
14. La chain tiene política de compactación.
15. Los huérfanos se borran de forma segura después de un grace period.

## 25. Principios para decisiones futuras

Ante varias soluciones válidas, en este orden: menos componentes · menos estado distribuido
· menos código propio en el data path · formatos inmutables · operaciones idempotentes ·
recovery explícito y testeable · optimizar sólo después de medir.

Una pieza nueva resuelve un problema observado o un requisito inmediato. No se incorpora una
abstracción porque "más adelante puede ser útil".

---

## 26. Resumen

```text
QEMU escribe qcow2 local.

Commit: flush → QMP external snapshot → nuevo active tip → el viejo queda sealed
        → sellar con la DEK → upload → manifest inmutable → CAS de HEAD

PostgreSQL: ownership, epoch, metadata
S3:         layers, commits, HEAD
Recovery:   PostgreSQL + S3 → reconstruir chain → nuevo active tip → iniciar VM
```

La prioridad inmediata es un ciclo completo y confiable:

```text
RUN → COMMIT → DESTROY HOST → RECOVER → RUN
```

Todo lo que no contribuya directamente a ese ciclo es secundario.

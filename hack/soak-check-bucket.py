"""Assert that whatever HEAD names is fully there.

This is the commit contract read from outside every process that wrote it: HEAD names a
commit, that commit's manifest is present and framed correctly, its layer is present and
hashes to what the manifest recorded, and so does every ancestor. It is the one thing that
must hold no matter which instruction a host was killed on.
"""

import hashlib
import json
import pathlib
import sys


def unframe(path):
    body = path.read_bytes()
    line, _, payload = body.partition(b"\n")
    want = line.decode()
    got = hashlib.sha256(payload).hexdigest()
    if got != want:
        sys.exit(f"{path}: digest line says {want}, contents hash to {got}")
    return json.loads(payload)


def main():
    store, volume = pathlib.Path(sys.argv[1]), sys.argv[2]
    head_path = store / "volumes" / volume / "HEAD"
    if not head_path.exists():
        print(f"no HEAD for {volume}: nothing was ever committed, which is not a broken chain")
        return
    head = unframe(head_path)
    if head["volume_id"] != volume:
        sys.exit(f"HEAD under {volume} names volume {head['volume_id']}")
    commit_id, depth, seen = head["commit_id"], 0, set()
    while commit_id:
        if commit_id in seen:
            sys.exit(f"the chain from HEAD loops at {commit_id}")
        seen.add(commit_id)
        manifest_path = store / "volumes" / volume / "commits" / f"{commit_id}.json"
        if not manifest_path.exists():
            sys.exit(f"HEAD's chain reaches {commit_id} and there is no manifest for it")
        m = unframe(manifest_path)
        if m["volume_id"] != volume or m["commit_id"] != commit_id:
            sys.exit(f"{manifest_path} describes {m['volume_id']}/{m['commit_id']}")
        layer = store / m["layer"]["object_key"]
        if not layer.exists():
            sys.exit(f"commit {commit_id} names layer {m['layer']['object_key']} and it is not there")
        if layer.stat().st_size != m["layer"]["size"]:
            sys.exit(f"layer {layer.name} is {layer.stat().st_size} bytes, manifest says {m['layer']['size']}")
        got = hashlib.sha256(layer.read_bytes()).hexdigest()
        if got != m["layer"]["sha256"]:
            sys.exit(f"layer {layer.name} hashes to {got}, manifest says {m['layer']['sha256']}")
        depth += 1
        commit_id = m.get("parent_commit_id", "")
    print(f"HEAD's chain is {depth} commits and every layer is present and matches its digest")


main()

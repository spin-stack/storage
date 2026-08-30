#!/usr/bin/env bash
# Deletes the buckets an AWS conformance run left behind.
#
# The suite purges its own buckets from t.Cleanup, so this is only for a run that
# was killed: a versioned bucket keeps every version and delete marker, and one
# that still holds a version cannot be deleted. Buckets are global and cost money
# until removed, and a leftover nobody recognizes is one nobody dares touch —
# which is why every bucket carries the spin-conf- prefix.
#
# Dry run by default. CONFIRM=yes actually deletes.
set -euo pipefail

prefix="spin-conf-"
confirm="${CONFIRM:-no}"

mapfile -t buckets < <(aws s3api list-buckets \
  --query "Buckets[?starts_with(Name, \`${prefix}\`)].Name" --output text | tr '\t' '\n' | grep -v '^$' || true)

if [ ${#buckets[@]} -eq 0 ]; then
  echo "no ${prefix}* buckets left behind"
  exit 0
fi

for b in "${buckets[@]}"; do
  if [ "$confirm" != "yes" ]; then
    echo "would delete $b"
    continue
  fi
  echo "deleting $b"
  doomed=$(aws s3api list-object-versions --bucket "$b" \
    --query '{Objects: ([Versions, DeleteMarkers][] || `[]`)[].{Key:Key,VersionId:VersionId}}' \
    --output json)
  if [ "$(echo "$doomed" | tr -d ' \n')" != '{"Objects":null}' ]; then
    # Plain delete first: S3 rejects the bypass header outright on a bucket
    # without Object Lock, which is most of them. The retry is for the one the
    # object-lock test leaves under GOVERNANCE retention.
    aws s3api delete-objects --bucket "$b" --delete "$doomed" >/dev/null ||
      aws s3api delete-objects --bucket "$b" --bypass-governance-retention --delete "$doomed" >/dev/null
  fi
  aws s3api delete-bucket --bucket "$b"
done

if [ "$confirm" != "yes" ]; then
  echo
  echo "re-run with CONFIRM=yes to delete these ${#buckets[@]} buckets"
fi

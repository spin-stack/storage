#!/usr/bin/env bash
# The two artefacts CI's guest-backed jobs need and this repository does not build: the
# pinned QEMU runtime image and the pinned guest kernel. One command publishes both.
#
# WHY THIS EXISTS. Publishing was documented rather than automated, across four files, and
# most of the ways to get it wrong are silent: a path CI does not resolve, the version tag
# instead of the content hash, a package the repository's Actions token cannot read. All
# three end as "not published" — the message printed when nobody published anything.
#
#   check     the preconditions, each with its remedy. No side effects.
#   publish   check, publish both, then prove it by resolving them the way ci.yml does.
#             Idempotent: re-run after the QEMU build finishes and it verifies.
#
# It does not build QEMU locally: qemu.yml builds the same artefact on a runner with the
# shared BuildKit cache and has workflow_dispatch. The kernel is the opposite case —
# storage never builds one (ADR-0021), so having it is a precondition, not a step.
#
# Nothing below has been run against a real registry; every precondition has been
# exercised in both states.
set -euo pipefail

cd "$(dirname "$0")/.."

# The registry is ghcr.io and only ghcr.io: ci.yml resolves both artefacts under
# `ghcr.io/${{ github.repository }}`, and a second registry here would be configuration for
# a need nobody has, whose first symptom is a lane resolving a path nothing published to.
readonly REGISTRY=ghcr.io

# Overridable so a caller can point at a different binary — the same reason DEADCODE and
# TASK_EXE are overridable elsewhere in hack/. It is also how the precondition checks were
# exercised in the "everything present" state on a machine whose gh token lacks the scope.
GH=${GH:-gh}
DOCKER=${DOCKER:-docker}
TASK_EXE=${TASK_EXE:-task}

# Supplied by the Taskfile (the single place each is declared); defaulted so the script is
# runnable by hand while still failing loudly on a value it cannot derive.
REPO=${REPO:-}
KERNEL=${KERNEL:-_output/guest/vmlinux}
KERNEL_SHA256=${KERNEL_SHA256:-}
KERNEL_VERSION=${KERNEL_VERSION:-}
SPINBOX_KERNEL=${SPINBOX_KERNEL:-}
QEMU_VERSION=${QEMU_VERSION:-}

# Resolved by check_repository, used by everything after it.
qemu_image=""
kernel_image=""
kernel_source="" # the file publish will push: $KERNEL or $SPINBOX_KERNEL

failures=0
ok() { printf '  ok       %s\n' "$1"; }
# A check that could not run says so, rather than printing nothing. Silence is how a
# reader concludes that everything above the failure was fine: the preflight this replaced
# had four states in which it reported OK having proven nothing, and this is the same trap
# one file over. It is not counted as a failure — the input it depends on already was.
skipped() { printf '  SKIPPED  %s\n' "$1"; }
missing() {
	printf '  MISSING  %s\n' "$1"
	shift
	local line
	for line in "$@"; do printf '           %s\n' "$line"; done
	failures=$((failures + 1))
}

sha256() { sha256sum "$1" | cut -d' ' -f1; }

# --- preconditions ------------------------------------------------------------------
#
# Each one is a thing that has to be true before publishing can work, and each failure
# names the input and the command that fixes it. The order is the order a human would hit
# them, so the first MISSING is usually the only one that matters.

check_repository() {
	if [ -z "$REPO" ]; then
		# `gh repo view` reads the git remote. This repository's remote is not always a
		# GitHub URL (a local bare repo is a perfectly good origin), so the fallback is
		# not an error condition — it is the normal case for a checkout that was never
		# cloned from GitHub, and the remedy is one variable.
		REPO=$($GH repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)
	fi
	if [ -z "$REPO" ]; then
		missing "the repository (owner/name)" \
			"nothing to name the packages after: 'gh repo view' could not resolve one from" \
			"this checkout's git remote, and REPO was not set." \
			"remedy: task guest:inputs:publish REPO=<owner>/<repo>"
		return
	fi
	if [[ ! $REPO =~ ^[^/]+/[^/]+$ ]]; then
		missing "the repository (owner/name)" \
			"REPO=$REPO is not <owner>/<repo>" \
			"remedy: task guest:inputs:publish REPO=<owner>/<repo>"
		return
	fi
	# Both refs are computed here, from the same sources ci.yml computes them from — the
	# Taskfile's QEMU_VERSION and `task guest:kernel:tag`. Nothing in this script spells a
	# version or a tag format out a second time: a path published here that the workflow
	# does not resolve is indistinguishable, from the workflow's side, from nothing having
	# been published.
	qemu_image="$REGISTRY/$REPO/qemu:$QEMU_VERSION"
	kernel_image="$REGISTRY/$REPO/guest-kernel:$($TASK_EXE guest:kernel:tag)"
	ok "repository $REPO"
	ok "target $qemu_image"
	ok "target $kernel_image"
}

check_gh() {
	command -v "$GH" >/dev/null 2>&1 || {
		missing "the gh CLI" \
			"needed to dispatch the QEMU workflow and to authenticate the registry push" \
			"remedy: install GitHub CLI (https://cli.github.com)"
		return
	}
	# One request answers three questions: is the token valid, whose is it, and what may it
	# do. The scopes come from the response header rather than from `gh auth status`'s
	# prose, because the prose is for a human and has changed shape between releases.
	local head
	head=$($GH api -i user 2>/dev/null) || {
		missing "a working GitHub login" \
			"gh could not authenticate (no token, expired token, or no network)" \
			"remedy: gh auth login"
		return
	}
	local scopes
	scopes=$(grep -i '^x-oauth-scopes:' <<<"$head" | tr -d '\r' | cut -d' ' -f2- || true)
	ok "gh authenticated as $($GH api user -q .login 2>/dev/null || echo '?')"
	# write:packages is what publishing a container to ghcr.io needs, and this script logs
	# Docker in with gh's token so the human never types a `docker login` — which means the
	# scope is this script's precondition and not a note in a document. A token without it
	# fails at the push, after the kernel has been built into an image, with a 403 that
	# reads like a permissions problem with the repository.
	case ",${scopes// /}," in
	*,write:packages,*) ok "token carries write:packages" ;;
	*) missing "write:packages on the gh token" \
		"scopes are: ${scopes:-<none>}" \
		"remedy: gh auth refresh -h github.com -s write:packages" ;;
	esac
}

check_repo_write() {
	[ -n "$REPO" ] || {
		skipped "write access — no repository resolved, so there is nothing to ask about"
		return 0
	}
	command -v "$GH" >/dev/null 2>&1 || {
		skipped "write access to $REPO — gh is what would answer it"
		return 0
	}
	local push
	push=$($GH api "repos/$REPO" -q .permissions.push 2>/dev/null || true)
	case "$push" in
	true) ok "write access to $REPO (workflow_dispatch is allowed)" ;;
	false) missing "write access to $REPO" \
		"workflow_dispatch on qemu.yml needs it, and so does publishing packages under" \
		"this repository's namespace." \
		"remedy: ask for write access, or publish from an account that has it" ;;
	*) missing "write access to $REPO" \
		"the repository did not answer — it may not exist on GitHub yet, or the token" \
		"cannot see it. gh api repos/$REPO returned nothing usable." \
		"remedy: create the repository, or set REPO=<owner>/<repo> to the right one" ;;
	esac
}

# The QEMU half is a dispatch, and a dispatch resolves the workflow on the *default
# branch*, not in this checkout. So a qemu.yml that exists here and has not been pushed —
# or that has been disabled in the Actions UI — fails at `gh workflow run` with a 404 that
# reads as though the command were wrong. This turns it into a named precondition.
check_qemu_workflow() {
	[ -n "$REPO" ] || {
		skipped "the QEMU workflow — no repository resolved"
		return 0
	}
	command -v "$GH" >/dev/null 2>&1 || {
		skipped "the QEMU workflow on $REPO — gh is what would answer it"
		return 0
	}
	local state
	state=$($GH api "repos/$REPO/actions/workflows/qemu.yml" -q .state 2>/dev/null || true)
	case "$state" in
	active) ok "qemu.yml is dispatchable on $REPO" ;;
	"") missing "qemu.yml on $REPO's default branch" \
		"a workflow_dispatch resolves the workflow on the default branch, not in this" \
		"checkout, so an unpushed .github/workflows/qemu.yml cannot be dispatched." \
		"remedy: push .github/workflows/qemu.yml to the default branch" ;;
	*) missing "qemu.yml on $REPO is $state, not active" \
		"a disabled workflow accepts no dispatch." \
		"remedy: re-enable it under the repository's Actions tab" ;;
	esac
}

check_docker() {
	command -v "$DOCKER" >/dev/null 2>&1 || {
		missing "docker" \
			"the kernel is published as a one-file image built with buildx" \
			"remedy: install Docker"
		return
	}
	if $DOCKER info >/dev/null 2>&1; then
		ok "docker daemon reachable"
	else
		missing "a reachable docker daemon" \
			"docker is installed but 'docker info' failed" \
			"remedy: start the daemon (systemctl start docker), or fix DOCKER_HOST"
	fi
}

# The one precondition that genuinely needs a human, and the reason this task cannot be a
# workflow: storage does not build a kernel (ADR-0021). It has to come off a machine that
# has spinbox's already-built artefact, either at the canonical path (a previous `task
# guest:kernel:fetch`) or in a sibling checkout.
check_kernel() {
	if [ -z "$KERNEL_SHA256" ]; then
		missing "GUEST_KERNEL_SHA256" \
			"there is no pin, so nothing could say whether the right kernel was published" \
			"remedy: set GUEST_KERNEL_SHA256 in Taskfile.yml (task guest:kernel:pin -- <file>)"
		return
	fi
	local candidate
	for candidate in "$KERNEL" "$SPINBOX_KERNEL"; do
		[ -n "$candidate" ] && [ -f "$candidate" ] || continue
		if [ "$(sha256 "$candidate")" = "$KERNEL_SHA256" ]; then
			kernel_source=$candidate
			ok "the pinned kernel ${KERNEL_VERSION:+$KERNEL_VERSION }at $candidate"
			return
		fi
		# A file that is there and is the wrong one is worth saying out loud: it is how a
		# lane certifies one kernel and publishes another.
		printf '           %s is %s, not the pinned %s\n' "$candidate" "$(sha256 "$candidate")" "$KERNEL_SHA256"
	done
	missing "the pinned guest kernel (sha256 $KERNEL_SHA256)" \
		"looked at $KERNEL and ${SPINBOX_KERNEL:-<SPINBOX_KERNEL unset>}." \
		"storage does not build a kernel (ADR-0021): it has to come from a machine with a" \
		"sibling spinbox checkout that has built the pinned one." \
		"remedy: cd ../spinbox && task build:kernel   # then re-run this task" \
		"         (it may exit non-zero and still emit the artefact — cache permissions)"
}

cmd_check() {
	echo "preconditions for publishing the guest-backed lanes' inputs:"
	check_repository
	check_gh
	check_repo_write
	check_qemu_workflow
	check_docker
	check_kernel
	echo
	if [ "$failures" -ne 0 ]; then
		echo "$failures precondition(s) missing — nothing was published." >&2
		return 1
	fi
	echo "OK: everything publishing needs is present. Run: task guest:inputs:publish"
}

# --- publish ---------------------------------------------------------------------------

# published <ref> — is it already in the registry? Used to decide what to do and, at the
# end, to prove what was done. `docker manifest inspect` is what ci.yml's preflight uses,
# so the postcondition here is the same question the workflow asks.
published() { $DOCKER manifest inspect "$1" >/dev/null 2>&1; }

# readable_by_ci <image-ref> — the question `published` cannot answer, and the reason a
# whole afternoon went into a package that existed.
#
# `docker manifest inspect` here runs as the human, with a personal token, so it succeeds
# for a package CI cannot read at all. From the workflow's side a private package linked
# to no repository is indistinguishable from one that was never pushed — same message,
# "not published". From this side they are perfectly distinguishable, so this is where the
# distinction belongs.
#
# **The linking claim in this file and in the Taskfile was too strong.** Both pushes carry
# org.opencontainers.image.source and that label is what links a package — *when Actions
# is the pusher*. Pushed from a laptop with a personal token, ghcr creates the package
# owned by the pusher, unlinked, and the label does not link it afterwards. Observed:
# storage/qemu, pushed by the QEMU workflow, came out linked; storage/guest-kernel, pushed
# from a developer machine minutes earlier with the same label, did not.
readable_by_ci() {
	local pkg="$1" owner="${REPO%%/*}" name repo vis
	pkg="${pkg#"$REGISTRY"/"$owner"/}"; pkg="${pkg%%:*}"
	name="${REPO#*/}/${pkg#*/}"
	# The org endpoint answers for a package the workflow cannot see; that asymmetry is
	# the whole point. A user-owned repository answers on /user/packages instead.
	local path="/orgs/$owner/packages/container/${name//\//%2F}"
	vis=$($GH api "$path" -q '.visibility // "unknown"' 2>/dev/null) || return 0
	repo=$($GH api "$path" -q '.repository.full_name // ""' 2>/dev/null) || repo=""
	if [ "$vis" = "public" ] || [ -n "$repo" ]; then
		return 0
	fi
	cat <<EOF

  the package exists and CI still cannot read it:
    $name is $vis and is linked to no repository.
  A workflow's GITHUB_TOKEN can read a private package only through the repository it
  belongs to, so from CI this is indistinguishable from never having been pushed — which
  is exactly the message you will get.
  It was pushed from here rather than by Actions, and org.opencontainers.image.source
  links a package only when Actions is the pusher.
  Fix it in two clicks — make it public, which is what the rest of this org's packages
  are and costs nothing for a kernel binary:
    https://github.com/orgs/$owner/packages/container/${name//\//%2F}/settings
EOF
	return 1
}

cmd_publish() {
	cmd_check

	echo
	# The registry login is done here rather than left to the human, because "did you
	# docker login" is the precondition nobody can check and everybody forgets. gh's token
	# is already the credential; ghcr.io accepts it as the password for the gh account.
	echo "logging in to $REGISTRY as $($GH api user -q .login)..."
	$GH auth token | $DOCKER login "$REGISTRY" -u "$($GH api user -q .login)" --password-stdin

	local qemu_dispatched=0
	echo
	if published "$qemu_image" && readable_by_ci "$qemu_image"; then
		echo "QEMU runtime image: already published ($qemu_image)"
	else
		# Dispatch rather than build. A local `task qemu:push` is tens of minutes
		# with a cold cache and produces the same image the workflow does — from the same
		# Taskfile target, with the same pinned version — while the workflow has the shared
		# BuildKit cache and does not hold a laptop hostage.
		local branch
		branch=$($GH api "repos/$REPO" -q .default_branch)
		echo "QEMU runtime image: not published — dispatching .github/workflows/qemu.yml on $branch"
		$GH workflow run qemu.yml --repo "$REPO" --ref "$branch"
		qemu_dispatched=1
	fi

	echo
	if published "$kernel_image"; then
		echo "guest kernel: already published ($kernel_image)"
	else
		echo "guest kernel: publishing $kernel_source as $kernel_image"
		# Through the Taskfile target, not through a second buildx invocation: the image's
		# shape (one file on scratch, the source label that links the package to the
		# repository) is defined once, at `guest:kernel:push`, and `task guest:kernel:fetch`
		# depends on that shape.
		$TASK_EXE guest:kernel:push \
			"GUEST_KERNEL_IMAGE=$REGISTRY/$REPO/guest-kernel" \
			"SPINBOX_KERNEL=$kernel_source"
	fi

	echo
	echo "verifying, the way .github/workflows/ci.yml's guest job does:"
	local unpublished=0
	if published "$kernel_image"; then
		# Not just "the manifest resolves": pull the kernel back out of the registry into a
		# scratch path and re-hash it against the pin. That is the whole round trip the
		# guest jobs make, and it is the only check that would catch an image carrying
		# something other than the kernel this repository pinned.
		local tmp
		tmp=$(mktemp -d)
		# shellcheck disable=SC2064  # $tmp must expand now, not at trap time
		trap "rm -rf '$tmp'" RETURN
		KERNEL="$tmp/vmlinux" KERNEL_SHA256="$KERNEL_SHA256" KERNEL_VERSION="$KERNEL_VERSION" \
			SPINBOX_KERNEL="" KERNEL_IMAGE="$kernel_image" bash hack/guest-kernel.sh fetch
		# And then the question the round trip cannot ask: this pull used a personal
		# token, so it succeeds for a package CI cannot read at all.
		if readable_by_ci "$kernel_image"; then
			echo "  ok       $kernel_image — pulled back and re-hashed against the pin, and readable by CI"
		else
			echo "  UNREADABLE  $kernel_image — the bytes are right and the workflow still cannot see them"
			unpublished=1
		fi
	else
		echo "  MISSING  $kernel_image"
		unpublished=1
	fi
	if published "$qemu_image" && readable_by_ci "$qemu_image"; then
		echo "  ok       $qemu_image"
	else
		echo "  MISSING  $qemu_image"
		[ "$qemu_dispatched" -eq 1 ] &&
			echo "           the build is running: $GH run watch --repo $REPO \$($GH run list --repo $REPO --workflow qemu.yml --limit 1 --json databaseId -q '.[0].databaseId')"
		unpublished=1
	fi

	echo
	if [ "$unpublished" -ne 0 ]; then
		# Non-zero, even when everything this run could do succeeded. The sentence a human
		# will repeat is "I ran the publish task", and that must not be able to mean "the
		# artefacts are still not there". The QEMU build takes tens of minutes; re-run
		# this when it finishes, it is idempotent and will verify instead of republishing.
		cat >&2 <<EOF
NOT DONE: an input above is still unpublished, so CI's guest jobs will still fail.
Re-run this task when the QEMU workflow finishes — it publishes nothing twice:

  $TASK_EXE guest:inputs:publish REPO=$REPO
EOF
		return 1
	fi

	# The one thing this script cannot do: confirm in the UI that each package is linked to
	# the repository, which is what lets that repository's Actions token read it.
	cat <<EOF
Both inputs are published and resolve the way the workflow resolves them.

One thing is left, and it is in the GitHub UI:
  https://github.com/$REPO/pkgs/container/${REPO#*/}%2Fqemu
  https://github.com/$REPO/pkgs/container/${REPO#*/}%2Fguest-kernel

Confirm each package is linked to this repository and that the repository has Read
access ("inherit access from source repository"). Both pushes carry
org.opencontainers.image.source — **and that label links a package only when Actions is
the pusher.** Pushed from here with a personal token, ghcr creates the package owned by
the pusher and unlinked, and adding the label does not link it afterwards. That was
observed rather than assumed: storage/qemu, pushed by the QEMU workflow, came out linked;
storage/guest-kernel, pushed from a developer machine minutes earlier with the same label,
did not. `readable_by_ci` above is what turns that into a named failure with a URL rather
than into "not published".

A pull request from a fork gets a token that cannot read a private package either; if
forks must pass the gate, make both packages public.

Then: any push runs .github/workflows/ci.yml, and its guest job stops failing.
EOF
}

case "${1:-}" in
check) cmd_check ;;
publish) cmd_publish ;;
*)
	echo "usage: $0 {check|publish}" >&2
	exit 2
	;;
esac

# k6 does not support Server-Sent Events natively. Section 2.6 records this as a
# practical constraint on any comparison of this kind, and notes that Mackovic
# (2025) had to build against the extension-capable xk6 variant with a community
# SSE extension in order to generate SSE load at all.
#
# This image performs that build, so all four arms are driven by ONE load
# generator with ONE metrics pipeline. The alternative -- k6 for three arms and
# a different tool for SSE -- would make the SSE latency figures incomparable
# with the others, which is the whole object of the study.
#
# ---------------------------------------------------------------------------
# VERSION SELECTION, and why each pin is the value it is
# ---------------------------------------------------------------------------
# These four versions are mutually constrained; none may be bumped alone.
#
#   K6_VER=v1.2.1  (deliberately NOT named K6_VERSION -- see note below)
#       NOT the newest k6. xk6-sse v0.1.12 declares `go.k6.io/k6 v1.2.1` in its
#       go.mod, and an extension built against a different k6 core is not
#       guaranteed to compile or to behave identically. The extension is the
#       binding constraint, so k6 is pinned to what the extension targets.
#       v1.2.1 also still supports --summary-export, which the orchestrator
#       relies on and which the k6 source marks for removal by v2.0.
#
#   XK6_SSE_VER=v0.1.12
#       Latest release of the community SSE extension at time of build.
#
#   XK6_VER=v1.4.9
#       Pinned explicitly. An earlier revision of this file used `@latest`,
#       which resolved to a build tool requiring a newer Go than the base image
#       provided and broke the build -- and, more importantly, contradicted the
#       reproducibility claim in Section 3.10. An unpinned dependency in a
#       reproducibility harness is a defect regardless of whether it happens to
#       build today.
#
#   GO_VERSION=1.25
#       Required by xk6 v1.4.9 (`requires go >= 1.25.0`).
#
# A NAMING TRAP worth recording, because it cost a build:
#   Docker exposes ARG values to RUN commands as ENVIRONMENT VARIABLES, and
#   xk6 v1.x binds --k6-version to the K6_VERSION environment variable. An
#   `ARG K6_VERSION` therefore silently sets the flag, and passing the version
#   positionally as well aborts with "k6 version was specified with both flag
#   and argument". The build arg is named K6_VER here so it cannot collide, and
#   the version is passed explicitly via --k6-version rather than positionally.
#
# The resolved versions are written into /versions.txt inside the final image:
#   docker run --rm realtime-bench/k6-sse:1.0.0 --version
#   docker run --rm --entrypoint cat realtime-bench/k6-sse:1.0.0 /versions.txt
# Record that output in the dissertation's environment table.
# ---------------------------------------------------------------------------

ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-bookworm AS build

ARG K6_VER=v1.2.1
ARG XK6_VER=v1.4.9
ARG XK6_SSE_VER=v0.1.12

# GOTOOLCHAIN=auto lets Go fetch a newer toolchain if a pinned dependency needs
# one. With XK6_VERSION pinned the requirement is deterministic, so this stays
# reproducible; it exists so that a future version bump fails with a clear
# message rather than the opaque GOTOOLCHAIN=local error.
ENV GOTOOLCHAIN=auto

RUN go install go.k6.io/xk6/cmd/xk6@${XK6_VER}

RUN xk6 build \
      --k6-version ${K6_VER} \
      --with github.com/phymbert/xk6-sse@${XK6_SSE_VER} \
      --output /usr/local/bin/k6

# Record exactly what was built, so the artefact documents itself.
RUN printf 'k6=%s\nxk6=%s\nxk6-sse=%s\ngo=%s\nbuilt=%s\n' \
      "${K6_VER}" "${XK6_VER}" "${XK6_SSE_VER}" \
      "$(go version)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /versions.txt

# Fail the build HERE rather than at run time if the SSE extension did not link.
# A k6 binary silently missing k6/x/sse would reduce the study from four arms to
# three, and the failure would not surface until the SSE cells of the sweep --
# potentially hours in, with the results of those cells simply absent.
RUN set -eux; \
    /usr/local/bin/k6 version; \
    printf 'import sse from "k6/x/sse";\nexport default function () {}\n' > /tmp/ssecheck.js; \
    if /usr/local/bin/k6 inspect /tmp/ssecheck.js > /dev/null 2>&1; then \
      echo "OK: k6/x/sse resolves"; \
    else \
      echo "FATAL: xk6-sse did not link into the k6 binary"; \
      /usr/local/bin/k6 inspect /tmp/ssecheck.js || true; \
      exit 1; \
    fi; \
    rm -f /tmp/ssecheck.js

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/k6 /usr/local/bin/k6
COPY --from=build /versions.txt /versions.txt
WORKDIR /scripts
COPY scripts ./
RUN k6 version && cat /versions.txt
ENTRYPOINT ["k6"]

# One image serves all four arms. Identical base, identical Node build,
# identical flags: the only thing that differs at run time is which server
# script the container starts and the environment it receives. Building four
# images would introduce a chance that layer or dependency drift is mistaken
# for a difference between architectures.
FROM node:22.14.0-bookworm-slim AS base

# Pinned to a patch release, not a tag that floats. A benchmark whose runtime
# can change between the pilot and the final sweep is not reproducible.

WORKDIR /app

# Dependency install as its own layer: ws only.
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev 2>/dev/null || npm install --omit=dev --no-audit --no-fund

COPY common ./common
COPY ws ./ws
COPY sse ./sse
COPY poll ./poll
COPY probe.js selftest.js ./

# V8 heap flags are pinned for ALL arms.
#
# Appelqvist & Ornmyr (2017) reported that inconsistency between garbage-
# collected and non-garbage-collected runs made a comparison of memory between
# technologies impossible. Section 2.7 identifies that as a confound requiring
# explicit control rather than hope. Pinning the heap bounds fixes when V8
# chooses to collect, making the memory series comparable across arms; the
# orchestrator additionally records a forced-GC baseline via --expose-gc so both
# figures are reported, as Chapter 3 specifies.
ENV NODE_OPTIONS="--max-old-space-size=2048 --max-semi-space-size=64 --expose-gc"

# Raise the descriptor ceiling inside the container. At the 1,000-client tier a
# default soft limit of 1,024 would produce an apparent architectural ceiling
# that is really a default setting -- exactly the artefact Kegel (2014) warns
# about and which must be reported alongside the configuration that produced it.
ENV UV_THREADPOOL_SIZE=8

EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=1s --retries=30 --start-period=2s \
  CMD node -e "require('http').get({host:'127.0.0.1',port:process.env.PORT||8080,path:'/health'},r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"

# Overridden per arm by compose.
CMD ["node", "ws/server.js"]

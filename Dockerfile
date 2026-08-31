# Copyright 2025 The KCP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM --platform=${BUILDPLATFORM} docker.io/golang:1.26.4 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /go/src/github.com/kcp-dev/api-syncagent
COPY . .
RUN GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} make clean kcp-cache-syncagent

FROM gcr.io/distroless/static-debian12:debug
LABEL org.opencontainers.image.source=https://github.com/gman0/kcp-cache-syncagent
LABEL org.opencontainers.image.description="A Kubernetes agent to synchronize data between kcp cache servers"
LABEL org.opencontainers.image.licenses=Apache-2.0

COPY --from=builder /go/src/github.com/gman0/kcp-cache-syncagent/_build/kcp-cache-syncagent /usr/local/bin/kcp-cache-syncagent

USER nobody
ENTRYPOINT [ "kcp-cache-syncagent" ]

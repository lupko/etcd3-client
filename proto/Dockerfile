FROM bufbuild/buf:1.11.0

RUN apk add --update --no-cache \
    protobuf \
    python3 \
    py3-pip \
    grpc \
    grpc-dev \
    && \
  rm -rf /var/cache/apk/* && pip3 install mypy-protobuf

ENTRYPOINT ["/usr/local/bin/buf"]

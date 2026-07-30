FROM tomsnow1999/docker-com_wechat_robot@sha256:14a96f3b0037976c7402f29e00698184ca103e7c88b13bead9fb5064e31e5e69

USER root
WORKDIR /

ENV COMWECHAT_VERSION=3.9.12.16 \
    COMWECHAT_BRIDGE_ENABLED=false \
    COMWECHAT_BRIDGE_IN_HOST=0.0.0.0 \
    COMWECHAT_BRIDGE_IN_PORT=23456 \
    COMWECHAT_BRIDGE_API_HOST=0.0.0.0 \
    COMWECHAT_BRIDGE_API_PORT=19088 \
    COMWECHAT_API_PORT=18888 \
    COMWECHAT_BRIDGE_MAX_BUFFER=20000 \
    COMWECHAT_CONSUME_RATE_PER_SEC=5

COPY run.py comwechat_bridge.py healthcheck.py /

RUN python3 -m py_compile /run.py /comwechat_bridge.py /healthcheck.py && \
    chmod 0755 /run.py /healthcheck.py

EXPOSE 5905 19088

HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python3", "/healthcheck.py"]

ENTRYPOINT ["/bin/dumb-init", "--"]
CMD ["/run.py", "start"]

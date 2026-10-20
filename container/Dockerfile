# electric-slime runner. slime.py is stdlib-only, so the image is the interpreter plus three
# files: no RUN steps, which is what lets one build serve amd64 and arm64 without emulation.
# Base pinned by its multi-arch index digest (python 3.12.15, alpine, built 2026-10-01).
FROM python:3.12-alpine@sha256:1b668429b3511ab407d8e00648891631b0b1a4d7e15e3ca70f38ab5b91ad4ab4

LABEL org.opencontainers.image.title="electric-slime runner" \
      org.opencontainers.image.description="FCFS runner pool for the electric-slime swarm"

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY slime.py manifest.json genome.json /opt/electric-slime/
USER 65534:65534

ENTRYPOINT ["python3", "/opt/electric-slime/slime.py"]
CMD ["pool", "--coord", "http://coordinator.lan:8700", "--slots", "2"]

#!/usr/bin/env bash
# Exchange messages between R (pubsubr) and Python (formix-pubsub).
# Usage: PYTHONPATH=/path/to/pubsub inst/examples/interop.sh
set -euo pipefail

echo "== Python subscribes, R publishes"
python3 -c '
from pubsub import Channel, subscribe
ch = Channel("interop.+")
with ch:
    subscribe(ch, lambda m: print("Python received", m.topic, m.content, m.headers), timeout_seconds=2)
' &
sleep 0.5
Rscript -e 'library(pubsubr); publish("interop.from.r", charToRaw("bonjour"), list(n = 42L, f = 3.5, ok = TRUE, z = NULL, s = "é"))' >/dev/null
wait

echo "== R subscribes, Python publishes"
Rscript -e 'library(pubsubr); ch <- Channel("interop.="); invisible(with_channel(ch, subscribe(ch, function(m) { cat("R received", m$topic, rawToChar(m$content), "\n"); str(m$headers) }, 2)))' &
sleep 1
python3 -c 'from pubsub import publish; publish("interop.x", b"salut", {"a": 1, "b": None, "c": 2.5})'
wait

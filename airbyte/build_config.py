#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml"]
# ///
"""Build the config the `source-declarative-manifest` runner expects.

Manifest-only connectors don't take a `--manifest-path` flag; the runner reads
the manifest from the config under the `__injected_declarative_manifest` key.
This merges `manifest.yaml` into your real `secrets/config.json` and writes
`secrets/merged_config.json`, which the spec/check/discover/read verbs consume.

Re-run it whenever you edit `manifest.yaml` or `secrets/config.json`.

    uv run build_config.py
"""

import json
import pathlib

import yaml

HERE = pathlib.Path(__file__).parent
manifest = yaml.safe_load((HERE / "manifest.yaml").read_text())
config = json.loads((HERE / "secrets" / "config.json").read_text())
config["__injected_declarative_manifest"] = manifest

out = HERE / "secrets" / "merged_config.json"
out.write_text(json.dumps(config, indent=2))
print(f"wrote {out.relative_to(HERE)}")

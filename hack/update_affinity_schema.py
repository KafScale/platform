# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
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

"""Regenerate only affinity fields in the otherwise hand-maintained CRD.

Requires Go and PyYAML. Full controller-gen output is intentionally not used:
the shipped CRD contains compatibility fields not represented by the Go types.
"""

import argparse
from pathlib import Path
import subprocess
import tempfile
import yaml

ROOT = Path(__file__).resolve().parents[1]
CRD = ROOT / "deploy/helm/kafscale/crds/kafscaleclusters.yaml"
GENERATOR = "sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on schema drift")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="kafscale-affinity-") as tmp:
        subprocess.run([
            "go", "run", GENERATOR, "crd:maxDescLen=0", "paths=./api/v1alpha1",
            f"output:crd:dir={tmp}",
        ], cwd=ROOT, check=True)
        generated = yaml.safe_load((Path(tmp) / "kafscale.io_kafscaleclusters.yaml").read_text())
    properties = generated["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
    original = CRD.read_text()
    updated = original
    for workload in ("brokers", "etcd"):
        schema = properties[workload]["properties"]["affinity"]
        schema["description"] = (
            "Replaces the default soft pod anti-affinity. Omit for the default; "
            "use an empty object to disable affinity. No automatic merge."
            + (" Applies only to operator-managed etcd." if workload == "etcd" else "")
        )
        lines = updated.splitlines(keepends=True)
        section = lines.index(f"                {workload}:\n")
        insertion = next(i for i in range(section + 1, len(lines)) if lines[i] == "                  properties:\n") + 1
        # Remove an earlier generated field while preserving every other field.
        end_section = next((i for i in range(insertion, len(lines)) if lines[i].startswith("                ") and not lines[i].startswith("                 ") and lines[i].strip()), len(lines))
        for i in range(insertion, end_section):
            if lines[i] == "                    affinity:\n":
                end = i + 1
                while end < len(lines) and (not lines[end].strip() or lines[end].startswith("                      ")):
                    end += 1
                del lines[i:end]
                break
        block = "                    affinity:\n" + "".join(
            "                      " + line + "\n"
            for line in yaml.safe_dump(schema, sort_keys=False).splitlines()
        )
        lines.insert(insertion, block)
        updated = "".join(lines)
    if args.check:
        if updated != original:
            raise SystemExit("Affinity CRD schema is stale; run python3 hack/update_affinity_schema.py")
    else:
        CRD.write_text(updated)


if __name__ == "__main__":
    main()

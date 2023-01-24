import json
import shutil
from pathlib import Path

with open("groups.json", "r") as f:
    groups = json.load(f)

group2 = groups["group_2"]
root = Path("/fsx/shared/pilev2_local_deduped")
new_root = Path("/fsx/shared/pilev2/pilv2_group2")
for path in group2:
    if path.startswith("/"):
        pass
        # path = Path(path)
        # src = path
        # dst = new_root / "reddit"
        # dst.parent.mkdir(parents=True, exist_ok=True)
        # shutil.move(src, dst)
    else:
        src = root / path
        dst = new_root / path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dst)
import copy
import json
import shutil
from pathlib import Path

root = Path("/fsx/shared/pilev2/deduped_2_2/deduped_2_2/group_2_2/")

arrow_files = list(root.glob("*.arrow"))
print(len(arrow_files))
# read state.json
with open(root / "state.json") as f:
    state = json.load(f)
print(state.keys())
# split into 10 shards
shard_size = len(arrow_files) // 3
shards = []
sharded_states = []
for i, arrow_file in enumerate(arrow_files):
    if i % shard_size == 0:
        if len(shards) > 0:
            # copy state.json
            shard_state = copy.deepcopy(state)
            shard_state["_data_files"] = []
            for file in shards[-1]:
                shard_state["_data_files"].append({'filename': file.name})
            sharded_states.append(shard_state)
        shards.append([])
    shards[-1].append(arrow_file)
shard_state = copy.deepcopy(state)
shard_state["_data_files"] = []
for file in shards[-1]:
    shard_state["_data_files"].append({'filename': file.name})
sharded_states.append(shard_state)
print(len(shards), len(sharded_states))
assert len(shards) == len(sharded_states)
# print(sharded_states[0])

# create a new folder for each shard and copy the arrow files
for i, (shard, state) in enumerate(zip(shards, sharded_states)):
    # assert shard[0].name, state["_data_files"][0]["filename"])
    shard_path = root / f"shard_{i}"
    shard_path.mkdir()
    # print(len(shard))
    for j, arrow_file in enumerate(shard):
        assert arrow_file.name == state["_data_files"][j]["filename"]
        shutil.copy(arrow_file, shard_path)
    # write state.json
    with open(shard_path / "state.json", "w") as f:
        json.dump(state, f)
    # copy dataset_info.json
    shutil.copy(root / "dataset_info.json", shard_path)
    with open(shard_path / "state.json", "w") as f:
        json.dump(state, f)

# # ensure the sizes are correct
# assert sum(len(shard) for shard in shards) == len(arrow_files)

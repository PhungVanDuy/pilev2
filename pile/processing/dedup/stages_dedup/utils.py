import datasets
import logging

logging.basicConfig(level=logging.INFO)
class IterDataset:
    def __init__(self,dataset_paths:list[str]):
        """
        Iterating over a list of dataset paths
        """
        self.logger = logging.getLogger("IterDataset")
        self.dataset_path_list = dataset_paths
        self.len = self.len_calc()
    
    def __len__(self):
        return self.len
    
    def len_calc(self):
        self.logger.info("Calculating length of dataset")
        total_len =  sum(len(datasets.load_from_disk(path)) for path in self.dataset_path_list)
        self.logger.info(f"Length of dataset is {total_len}")
        return total_len

    def __iter__(self):
        for path in self.dataset_path_list:
            dataset = datasets.load_from_disk(path)
            for datapoint in dataset:
                yield datapoint




def generate_datapoint(shard_dataset_paths:list[str]):
    """
    Generating datapoint from a list of shard dataset paths
    """
    for shard_dataset_path in shard_dataset_paths:
        shard_dataset = datasets.load_from_disk(shard_dataset_path)
        for datapoint in shard_dataset:
            yield datapoint


if __name__ == "__main__":
    dataset_paths = [
        "/Users/reshinthadithyan/master/research/code-research/carperai/demo/pile-v2-eda/cache_ds/AI4Code",
        "/Users/reshinthadithyan/master/research/code-research/carperai/demo/pile-v2-eda/cache_ds/DMMath",
        "/Users/reshinthadithyan/master/research/code-research/carperai/demo/pile-v2-eda/cache_ds/PileOfLaw"
    ]
    embedded = datasets.iterable_dataset.IterableDataset.from_generator(generate_datapoint)
    #embedded = IterDataset(dataset_paths)
    embed_batch = embedded[:100]
    print(len(embed_batch))
import argparse
import pathlib
import pandas as pd




def find_all_arrow_files(path:str):
    path = pathlib.Path(path)
    return list(path.glob("**/*.arrow"))

def make_pandas_df_from_all_arrow_files(list_of_dataset_paths:str,output_path:str):
    arrow_files_path = []
    for dataset_path in list_of_dataset_paths:
        ind_arrow_dataset_path = find_all_arrow_files(dataset_path)
        arrow_files_path.extend(ind_arrow_dataset_path)
    arrow_files_path = [str(path) for path in arrow_files_path]
    dataframe = pd.DataFrame.from_dict({"path":arrow_files_path})
    dataframe.to_parquet(output_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_paths", type=str, nargs="+", required=True)
    parser.add_argument("--output_path", type=str, required=True)
    args = parser.parse_args()
    make_pandas_df_from_all_arrow_files(args.dataset_paths,args.output_path)

if __name__ == "__main__":
    main()
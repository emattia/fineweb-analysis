from metaflow import FlowSpec, step, kubernetes, pypi

class FinewebAnalysis(FlowSpec):

    ds_name='HuggingFaceFW/fineweb'

    @secrets(sources=['huggingface'])
    @step
    def start(self):
        from datasets import get_dataset_config_names
        import requests

        self.dump = get_dataset_config_names(self.ds_name)        
        self.next(self.process_cc_dump, foreach='dump')
    
    # EC2 --> r6id.32xlarge	
    # GCP --> m3-ultramem-128	
    # Azure --> Standard_D64s_v3
    @kubernetes(cpu=124, memory=1_000_000)
    @pypi(
        packages={
            'datasets': '1.9.0', 
            'duckdb': '0.2.8',
            'polars': '0.11.0',
            'requests': '2.26.0'
        }
    )
    @step
    def process_cc_dump(self):
        import os
        import psutil
        from datasets import load_dataset

        self.dump_name = self.input

        # Option 1: Load the dataset 
        self.mem_before = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        ds = load_dataset(self.ds_name, self.dump_name, split='train')
        self.mem_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

        # Option 2: Load the dataset in streaming mode
        ds_stream = load_dataset(self.ds_name, self.dump_name, split='train', streaming=True)

        # Get stats
        from utils import get_stats
        self.ds_stats = get_stats(self.ds_name, self.dump_name)

        # convert ds to duckdb and run queries

        ### Data Quality and Integrity ###

        # How consistent are the language scores across different samples?


        # Are there any anomalies or outliers in terms of token counts for certain samples?
        # How reliable are the ID fields? Are there any duplicates?
        # What is the range of dates in the dataset, and are there any gaps in the crawl dates?
                    
        self.start_date = ...
        self.end_date = ...

        self.next(self.join_stats)

    @pypi(packages={'pandas': '2.2.2'})
    @step
    def join_stats(self, inputs):
        import pandas as pd
        self.ds_stats = pd.DataFrame()
        self.dump_stats = pd.DataFrame()
        for input in inputs:
            _ds_stats = input.ds_stats
            _ds_stats['dump_name'] = [input.dump_name] * len(_ds_stats)
            self.ds_stats = pd.concat([self.ds_stats, _ds_stats])
            self.dump_stats = df.append({
                'dump_name': input.dump_name,
                'mem_before': input.mem_before,
                'mem_after': input.mem_after,
                'start_date': input.start_date,
                'end_date': input.end_date
            }, ignore_index=True)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    FinewebAnalysis()
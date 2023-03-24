import pandas as pd
from urllib.parse import urlparse
from example_pipeline.pipeline import pipeline
from pathlib import Path


def test_pipeline():
    source_dir = str(Path.cwd().parent)
    df = pd.read_csv(f'{source_dir}\\original\\original.csv')
    df['domain_of_url'] = [urlparse(i).netloc for i in df['url']]
    pipeline.run()
    df_from_pipeline = pd.read_csv(f'{source_dir}\\original\\norm.csv')
    comp = df.compare(df_from_pipeline)
    assert comp.isnull().sum().sum() == comp.size, "Incorrect result"
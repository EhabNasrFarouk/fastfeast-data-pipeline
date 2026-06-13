import yaml
import pandas as pd
from pathlib import Path

root = Path(__file__).resolve().parent.parent.parent
print(root)

config_path = root / 'config/config.yaml'
with open(config_path, 'r') as file:
    data = yaml.safe_load(file)


cities = root / data["Ingestion"]["Master"] / "cities.csv"
df = pd.read_csv(cities)
print(df.head())
from typing import Any, Dict
from nanolab_processing_base.extras.datasets.nanolab_dataframe import make_props_data
import pickle
from kedro.io.core import AbstractDataset


class NanoLabDataSet(AbstractDataset):
    def __init__(self, filepath: str, catalog: Any):
        super().__init__()
        self.filepath = filepath
        self.catalog = catalog
        self.props = None
        self.data = None

    def _load(self) -> Any:
        self.props, self.data = make_props_data(self.filepath)
        return self.props, self.data

    def _save(self, data: Any) -> None:
        with open(self.filepath, 'wb') as f:
            pickle.dump(data, f)

    def _describe(self) -> Dict[str, Any]:
        return {
            "type": "NanoLab Data Frame",
            "filepath": self.filepath,
        }

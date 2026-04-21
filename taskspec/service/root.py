from logging import getLogger
import yaml
import os
import glob
from typing import Dict

from ..executor import ExecutorServiceManager
from ..schema import SpecData
from .spec import SpecService

logger = getLogger(__name__)

class RootService:
    def __init__(self, base_dir: str, executor_mgr: ExecutorServiceManager, public_url: str):
        self._base_dir = base_dir
        self._executor_mgr = executor_mgr
        self._public_url = public_url
        self._spec_services: Dict[str, SpecService] = {}

    def init(self) -> None:
        specs_pattern = os.path.join(self._base_dir, 'specs', '*', 'config.yml')
        for config_path in glob.glob(specs_pattern):
            spec_dir = os.path.dirname(config_path)
            spec_name = os.path.basename(spec_dir)

            with open(config_path, 'r') as f:
                spec_dict = yaml.safe_load(f)

            # Instantiate TaskSpec in RootService
            spec_dict['name'] = spec_name
            spec = SpecData(**spec_dict)
            spec_dir = spec.get_dir(self._base_dir)

            if not spec.executor:
                logger.warning(f"Spec {spec_name} has no executor defined, skipping")
                continue

            executor = self._executor_mgr.get_executor(spec.executor)
            spec_service = SpecService(spec_name, spec_dir, spec, executor,
                                       public_url=self._public_url)
            spec_service.init()
            self._spec_services[spec_name] = spec_service
            logger.info(f"Loaded spec service: {spec_name}")

    def get_spec_service(self, spec_name: str) -> SpecService:
        if spec_name not in self._spec_services:
            raise ValueError(f"Spec service not found: {spec_name}")
        return self._spec_services[spec_name]

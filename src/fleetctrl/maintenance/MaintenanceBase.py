from __future__ import annotations
from abc import abstractmethod, ABC
import logging
from typing import Dict, List, Any, Tuple, TYPE_CHECKING

# from src.misc.globals import

if TYPE_CHECKING:
    from src.fleetctrl.FleetControlBase import FleetControlBase

LOG = logging.getLogger(__name__)


class MaintenanceBase(ABC):
    def __init__(self, fleetctrl: FleetControlBase, operator_attributes: dict, solver="Gurobi"):
        """Initialization of maintenance class

        :param: fleetctrl: FleetControl class
        :param operator_attributes: operator dictionary that can contain additionally required parameters
        :param solver: solver for optimization problems
        """
        self.fleetctrl = fleetctrl

    @abstractmethod
    def trip_triggered_maintenance_processes(self, sim_time):
        """This method can be used to apply maintenance strategy

        :param sim_time: current simulation time
        :return: None
        """

        pass

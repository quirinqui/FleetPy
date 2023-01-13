from __future__ import annotations
from abc import abstractmethod, ABC
import logging
from typing import Dict, List, Any, Tuple, TYPE_CHECKING

from src.misc.globals import G_OP_CLEAN_N_OFFER_P_ST_QUERY, G_OP_CLEAN_N_STATION_QUERY

if TYPE_CHECKING:
    from src.infra.MaintenanceInfrastructure import OperatorMaintenanceAndDepotInfrastructure, PublicMaintenanceInfrastructureOperator
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
        self.list_pub_maintenance_infra = fleetctrl.list_pub_maintenance_infra
        self.all_maintenance_infra: List[PublicMaintenanceInfrastructureOperator] = []
        self.all_maintenance_infra += self.list_pub_maintenance_infra[:]
        self.routing_engine = fleetctrl.routing_engine
        self.solver_key = solver
        self.n_stations_to_query = operator_attributes.get(G_OP_CLEAN_N_STATION_QUERY, 1)
        self.n_offers_p_station = operator_attributes.get(G_OP_CLEAN_N_OFFER_P_ST_QUERY, 1)
        self.target_clean = 1.0  # TODO

    @abstractmethod
    def time_triggered_maintenance_processes(self, sim_time):
        """This method can be used to apply maintenance strategy

        :param sim_time: current simulation time
        :return: None
        """

        pass

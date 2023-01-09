import logging

from src.fleetctrl.maintenance.MaintenanceBase import MaintenanceBase
# from src.fleetctrl.planning.VehiclePlan import
from src.misc.globals import *

LOG = logging.getLogger(__name__)

class MaintenanceThresholdPublicInfrastructure(MaintenanceBase):

    def __init__(self, fleetctrl, operator_attributes, solver="Gurobi"):
        super().__init__(fleetctrl, operator_attributes, solver=solver)
        # Threshold after which a maintenance/calibration process is triggered
        self.cleanliness_threshold = 0.1

    def trip_triggered_maintenance_processes(self, sim_time, veh_obj):
        LOG.debug("drop-off triggered removing cleanliness at {}".format(sim_time))

        # init values
        current_plan = self.fleetctrl.veh_plans[veh_obj.vid]
        is_maintenance_required = False
        last_time = sim_time
        last_pos = veh_obj.pos
        last_clean = veh_obj.clean
        #TODO calibration value

        # does the current vehicle haves planed routes?
        if current_plan.list_plan_stops:
            # get last stop of the planed routes
            last_pstop = current_plan.list_plan_stops[-1]
            LOG.debug(f"last ps of vid {veh_obj} : {last_pstop}")
            # TODO create function to be able to calculate maintenance values after routes
            # LOG.debug(f" state {last_pstop.get_state()} inactive {last_pstop.is_inactive()} arr dep soc {last_pstop}")
            # TODO create Value in Enum G_PLANSTOP_STATES MAINTENANCE
            # if not last_pstop.get_state() == G_PLANSTOP_STATES
        pass

import logging

from src.fleetctrl.maintenance.MaintenanceBase import MaintenanceBase
from src.simulation.StationaryProcess import MaintenanceProcess
from src.fleetctrl.planning.VehiclePlan import MaintenancePlanStop
from src.misc.globals import *

LOG = logging.getLogger(__name__)

class MaintenanceThresholdPublicInfrastructure(MaintenanceBase):

    def __init__(self, fleetctrl, operator_attributes, solver="Gurobi"):
        super().__init__(fleetctrl, operator_attributes, solver=solver)
        # Threshold after which a maintenance/calibration process is triggered
        self.cleanliness_threshold = operator_attributes.get(G_OP_APS_CLEAN, 0.1)

    def time_triggered_maintenance_processes(self, sim_time):
        LOG.debug("Q time triggered maintenance at {}".format(sim_time))

        for veh_obj in self.fleetctrl.sim_vehicles:
            # do not consider inactive vehicles
            if veh_obj.status in {VRL_STATES.OUT_OF_SERVICE, VRL_STATES.BLOCKED_INIT}:
                continue
            # init values
            current_plan = self.fleetctrl.veh_plans[veh_obj.vid]
            is_maintenance_required = False
            last_time = sim_time
            last_pos = veh_obj.pos
            last_clean = veh_obj.cleanliness

            # does the current vehicle haves planed routes?
            if current_plan.list_plan_stops:
                # get last stop of the planed routes
                last_pstop = current_plan.list_plan_stops[-1]
                LOG.debug(f"last ps of vid {veh_obj} : {last_pstop}")

                # TODO Q create function to be able to calculate maintenance values after routes
                # LOG.debug(f" state {last_pstop.get_state()} inactive {last_pstop.is_inactive()} arr dep clean {last_pstop.get.....}")
                # TODO Q create Value in Enum G_PLANSTOP_STATES MAINTENANCE
                if not last_pstop.get_state() == G_PLANSTOP_STATES and not last_pstop.is_inactive():
                    continue
                    # TODO Q last_clean must be available here
            pass

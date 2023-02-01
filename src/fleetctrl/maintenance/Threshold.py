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
        LOG.debug("time triggered maintenance at {}".format(sim_time))

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
                LOG.debug(f" state {last_pstop.get_state()} inactive {last_pstop.is_inactive()} arr dep clean {last_pstop.get_planned_arrival_and_departure_clean()}")
                if not last_pstop.get_state() == G_PLANSTOP_STATES.MAINTENANCE and not last_pstop.is_inactive():
                    _, last_clean = last_pstop.get_planned_arrival_and_departure_clean()
                    if last_clean < self.cleanliness_threshold:
                        maintenance_planned = False
                        for ps in current_plan.list_plan_stops:
                            if ps.get_state() == G_PLANSTOP_STATES.MAINTENANCE:
                                maintenance_planned = True
                                LOG.debug(" -> but maintenance is already planned")
                                break
                            if not maintenance_planned:
                                _, last_time = last_pstop.get_planned_arrival_and_departure_time()
                                last_pos = last_pstop.get_pos()
                                is_maintenance_required = True
            elif veh_obj.cleanliness < self.cleanliness_threshold:
                is_maintenance_required = True

            if is_maintenance_required is True:
                LOG.debug(f"maintenance required for vehicle {veh_obj}")
                best_maintenance_poss = None
                best_clean_op = None
                for clean_op in self.all_maintenance_infra:
                    maintenance_possibilities = clean_op.get_maintenance_slots(sim_time, veh_obj, last_time, last_pos,
                                                                               last_clean, self.target_clean,
                                                                               max_number_maintenance_stations=self.n_stations_to_query,
                                                                               max_offers_per_station=self.n_offers_p_station)
                    LOG.debug(f"maintenance possibilities of clean op {clean_op.clean_op_id} : {maintenance_possibilities}")
                    if len(maintenance_possibilities) > 0:
                        clean_op_best = min(maintenance_possibilities, key=lambda x:x[3])
                        if best_maintenance_poss is None or clean_op_best[3] < best_maintenance_poss[3]:
                            best_maintenance_poss = clean_op_best
                            best_clean_op = clean_op
                if best_maintenance_poss is not None:
                    LOG.debug(f" -> best maintenance possibility: {best_maintenance_poss}")
                    (station_id, spot_id, possible_start_time, possible_end_time, desired_veh_clean, max_maintenance_speed) = best_maintenance_poss
                    booking = best_clean_op.book_station(sim_time, veh_obj, station_id, spot_id, possible_start_time, possible_end_time)
                    station = best_clean_op.station_by_id[station_id]
                    start_time, end_time = booking.get_scheduled_start_end_times()
                    maintenance_task_id = (best_clean_op.clean_op_id, booking.id)
                    ps = MaintenancePlanStop(station.pos, earliest_start_time=start_time, duration=end_time-start_time,
                                             maintenance_speed=max_maintenance_speed, maintenance_task_id=maintenance_task_id, locked=True)
                    current_plan.add_plan_stop(ps, veh_obj, sim_time, self.routing_engine)
                    self.fleetctrl.lock_current_vehicle_plan(veh_obj.vid)
                    self.fleetctrl.assign_vehicle_plan(veh_obj, current_plan, sim_time,
                                                       assigned_maintenance_task=(maintenance_task_id, booking))

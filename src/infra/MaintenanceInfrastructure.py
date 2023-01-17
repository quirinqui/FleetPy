# -------------------------------------------------------------------------------------------------------------------- #
# standard distribution imports
# -----------------------------
from __future__ import annotations
import logging
import typing as tp
import time
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
from pandas import DataFrame
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from operator import attrgetter, itemgetter

# additional module imports (> requirements)
# ------------------------------------------

# src imports
# -----------
from pyproj import Transformer

from src.misc.globals import *
from src.simulation.StationaryProcess import MaintenanceProcess
from src.fleetctrl.planning.VehiclePlan import MaintenancePlanStop, VehiclePlan, RoutingTargetPlanStop
from src.misc.config import decode_config_str
if TYPE_CHECKING:
    from src.routing.NetworkBase import NetworkBase
    from src.simulation.Vehicles import SimulationVehicle
    from src.fleetctrl.FleetControlBase import FleetControlBase

# -------------------------------------------------------------------------------------------------------------------- #
# global variables
# ----------------
LOG = logging.getLogger(__name__)

MAX_MAINTENANCE_SEARCH = 100   # TODO # in globals?
LARGE_INT = 100000000

class MaintenanceSpot:
    """ This class represents a single maintenance spot """

    def __init__(self, spot_id, maintenance_speed):
        self.id = spot_id
        self.maintenance_speed = maintenance_speed
        self.attached_vehicle: Optional[SimulationVehicle] = None
        self.initial_cleanliness = None
        self.connect_time = None

    def __repr__(self):
        vehicle = None if self.attached_vehicle is None else self.attached_vehicle.vid
        return f"maintenance spot id: {self.id}, attached vehicle {vehicle}"


    @property
    def cleaned_amount(self):
        cleaned_amount_calc = 0
        if self.attached_vehicle is not None:
            cleaned_amount_calc = (self.attached_vehicle.cleanliness - self.initial_cleanliness)
        return cleaned_amount_calc

    def attach(self, sim_time, vehicle: SimulationVehicle):
        """ Attaches vehicle to the spot

        :param sim_time:    current simulation time
        :param vehicle:     simulation vehicle
        :return:            True if the spot was empty and the vehicle attaches successfully
        """

        is_attached = False
        if self.attached_vehicle is None:
            self.attached_vehicle = vehicle
            self.initial_cleanliness = vehicle.cleanliness
            self.connect_time = sim_time
            is_attached = True
        return is_attached

    def detach(self):
        assert self.attached_vehicle is not None, f"no vehicle to detach from spot {self.id}"
        self.attached_vehicle = None
        self.initial_cleanliness = None
        self.connect_time = None

    def clean_vehicle(self, delta_time):
        """ Linear cleaning of the attached vehicle

        :param delta_time: the time increment in seconds
        """
        delta_cleanliness = self.maintenance_speed * delta_time / 3600
        self.attached_vehicle.cleanliness = min(1.0, self.attached_vehicle.cleanliness + delta_cleanliness)

    def calculate_maintenance_duration(self, veh_object: SimulationVehicle, start_cleanliness, end_cleanliness) -> float:
        """ Calculates the maintenance duration in seconds required to clean the provided vehicle

        :param veh_object:      the vehicle object for which maintenance duration is required
        :param start_cleanliness:       the starting cleanliness of the vehicle.
        :param end_cleanliness:         The final cleanliness upto which the vehicle should be cleaned.
        """
        start_cleanliness = veh_object.cleanliness if start_cleanliness is None else start_cleanliness
        remaining_maintenance = (end_cleanliness - start_cleanliness)
        return remaining_maintenance / self.maintenance_speed * 3600


class MaintenanceStation:
    """ This class represents a public maintenance station with multiple maintenance spots """
    station_history = defaultdict(list)
    station_history_file_path = None

    def __init__(self, station_id, clean_op_id, node, spot_ids, maintenance_speed: List[float]):
        self.id = station_id
        self.clean_op_id = clean_op_id
        self.pos = (node, None, None)
        self._spots: Dict[int, MaintenanceSpot] = {id: MaintenanceSpot(id, max_cleaning_speed)
                                                       for id, max_cleaning_speed in zip(spot_ids, maintenance_speed)}
        self._sorted_spots = sorted(self._spots.values(), key=attrgetter("maintenance_speed"), reverse=True)
        self._vid_spot_dict: Dict[int, MaintenanceSpot] = {}

        # Dictionary for the maintenance schedule with booking ids as keys
        self._booked_processes: Dict[str, MaintenanceProcess] = {}
        self._spot_bookings: Dict[int, List[MaintenanceProcess]] = {ID: [] for ID in self._spots}
        self._current_processes: Dict[int, Optional[MaintenanceProcess]] = {ID: None for ID in self._spots}

    def __add_to_scheduled(self, booking: MaintenanceProcess):
        self._booked_processes[booking.id] = booking
        self._spot_bookings[booking.spot_id].append(booking)

    def __remove_from_scheduled(self, booking: MaintenanceProcess):
        del self._booked_processes[booking.id]
        self._spot_bookings[booking.spot_id].remove(booking)

    def calculate_maintenance_durations(self, veh_object: SimulationVehicle, start_cleanliness=None,
                                   end_cleanliness=1.0) -> Dict[int, float]:
        """ Calculates the maintenance duration in seconds required to clean the given vehicle

        :param veh_object:      the vehicle object for which maintenance duration is required
        :param start_cleanliness:       the starting cleanliness of the vehicle. By default the start_cleanliness is derived from current cleanliness
                                of the given veh_object
        :param end_cleanliness:         The final cleanliness upto which the vehicle should be cleaned. The default is full clean (1.0)
        :returns:   Dict with spot ids as keys and maintenance duration as values
        """

        start_cleanliness = veh_object.cleanliness if start_cleanliness is None else start_cleanliness
        return {spot.id: spot.calculate_maintenance_duration(veh_object, start_cleanliness, end_cleanliness)
                for spot in self._sorted_spots}

    def get_running_processes(self):
        """ Returns the currently running and scheduled maintenance processes

        :returns: - Dict of currently running maintenance processes with spot_id as key
                  - Dict of scheduled maintenance processes with spot_id as key
        """
        return self._current_processes.copy(), self._spot_bookings.copy()

    def get_current_schedules(self, sim_time) -> Dict[int, List[Tuple[float, float, str]]]:
        """ Returns the time slots already booked/occupied for each of the maintenance spots

        :param sim_time: current simulation time
        :returns:   - Dict with spot_id as keys and list of (start time, end time, booking id) as values
        """

        schedules = {spot_id: [] for spot_id in self._spots}
        for spot_id, current_booking in self._current_processes.items():
            if current_booking is not None:
                end_time = sim_time + current_booking.remaining_duration_to_finish(sim_time)
                schedules[spot_id].append((current_booking.start_time, end_time, current_booking.id))
        for spot_id, bookings in self._spot_bookings.items():
            for scheduled_booking in bookings:
                schedules[spot_id].append((scheduled_booking.start_time, scheduled_booking.end_time,
                                             scheduled_booking.id))
            schedules[spot_id] = sorted(schedules[spot_id], key=itemgetter(0))
        return schedules

    def start_maintenance_process(self, sim_time, booking: MaintenanceProcess) -> bool:
        """ Starts the provided maintenance process by connecting the vehicle to the spot

        :param sim_time:    Current simulation time
        :param booking:     The maintenance process instance to be started
        """

        spot = self._spots[booking.spot_id]
        found_empty_spot = spot.attach(sim_time, booking.veh)
        LOG.debug(f"start maintenance process: {booking} at time {sim_time}")
        LOG.debug(f"with schedule {self.get_current_schedules(sim_time)}")
        assert found_empty_spot is True, f"unable to connect to the spot {spot} at station {self.id}"
        self._vid_spot_dict[booking.veh.vid] = spot
        self._current_processes[spot.id] = booking
        self.__remove_from_scheduled(booking)
        return found_empty_spot

    def end_maintenance_process(self, sim_time, booking: MaintenanceProcess):
        spot = self._spots[booking.spot_id]
        assert spot.attached_vehicle.vid == booking.veh.vid
        self.__append_to_history(sim_time, booking.veh, "detach", spot)
        spot.detach()
        del self._vid_spot_dict[booking.veh.vid]
        self._current_processes[spot.id] = None

    def cancel_booking(self, sim_time, booking: MaintenanceProcess):
        if booking in self._current_processes.values():
            LOG.warning(f"canceling an already running maintenance process {booking.id} at station {self.id}")
            self.end_maintenance_process(sim_time, booking)
        else:
            self.__remove_from_scheduled(booking)

    def update_maintenance_state(self, booking: MaintenanceProcess, delta_time):
        """ Charges the vehicle according to delta_time """

        spot = self._spots[booking.spot_id]
        assert spot.attached_vehicle.vid == booking.veh.vid, "the vehicle attached to the spot and the vehicle " \
                                                               "in the booking are not same"
        spot.clean_vehicle(delta_time)

    def remaining_maintenance_time(self, sim_time, vid, end_cleanliness=1.0) -> float:
        """ Returns the remaining time in seconds required to clean the attached vehicle
        it is either cleaned until end_cleanliness or until the next process is scheduled
        :param sim_time: current simulation time
        :param vid: vehicle id
        :param end_cleanliness: the final cleanliness for which the remaining maintenance duration is required. Default is full cleanliness (1.0)
        """
        assert vid in self._vid_spot_dict, f"vehicle {vid} not attached to any spot at station {self.id}, " \
                                             f"remaining time cannot be calculated"
        attached_spot = self._vid_spot_dict[vid]
        full_maintenance_duration = attached_spot.calculate_maintenance_duration(attached_spot.attached_vehicle, attached_spot.attached_vehicle.cleanliness, end_cleanliness)
        LOG.debug(" -> full clean duration at time {}: {}".format(sim_time, full_maintenance_duration))
        # test for next bookings
        if len(self._spot_bookings.get(attached_spot.id, [])) > 0:
            next_start_time = min(self._spot_bookings[attached_spot.id], key=lambda x:x.start_time).start_time
            assert next_start_time - sim_time > 0, f"uncancelled bookings in maintenance station {self.id} at time {sim_time} with schedule { [str(c) for c in self._spot_bookings[attached_spot.id] ]}!"
            if next_start_time - sim_time < full_maintenance_duration:
                return next_start_time - sim_time
        return full_maintenance_duration

    def make_booking(self, sim_time, spot_id, vehicle: SimulationVehicle, start_time=None,
                     end_time=None) -> MaintenanceProcess:
        """ Makes a booking for the vehicle

        :param sim_time:    current simulation time
        :param spot_id:   id of the spot to be booked
        :param vehicle:     vehicle object
        :param start_time:  start time of the booking (default value is equal to sim_time)
        :param end_time:    end time of the maintenance. The default value (None) will clean the vehicle upto full cleanliness
        """

        start_time = sim_time if start_time is None else start_time
        booking_id = f"{self.id}_{spot_id}_{vehicle.vid}_{int(start_time)}_{int(end_time)}"
        booking = MaintenanceProcess(booking_id, vehicle, self, start_time, end_time)
        self.__add_to_scheduled(booking)
        return booking

    def modify_booking(self, sim_time, booking: MaintenanceProcess):
        raise NotImplemented
    
    def get_maintenance_slots(self, sim_time, vehicle, planned_arrival_time, planned_start_cleanliness, desired_end_cleanliness, max_offers_per_station=1):
        """ Returns specific maintenance possibilities for a vehicle at this maintenance station
        a future time, place with estimated cleanliness and desired cleanliness.

        :param sim_time: current simulation time
        :param vehicle: the vehicle for which the maintenance slot is required
        :param planned_arrival_time: earliest time at which maintenance should be considered
        :param planned_start_cleanliness: estimated vehicle cleanliness at that position
        :param desired_end_cleanliness: desired final cleanliness after maintenance
        :param max_offers_per_station: maximum number of offers per maintenance station to consider
        :return: list of specific offers of MaintenanceOperator consisting of
                    (maintenance station id, maintenance spot id, booking start time, booking end time, expected booking end cleanliness, max_maintenance_speed)
        """

        list_station_offers = []
        maintenance_durations = self.calculate_maintenance_durations(vehicle, planned_start_cleanliness, desired_end_cleanliness)
        estimated_arrival_time = planned_arrival_time
        for spot_id, spot_schedule in self.get_current_schedules(sim_time).items():
            spot_maintenance_duration = maintenance_durations[spot_id]
            possible_start_time = estimated_arrival_time
            found_free_slot = False
            maintenenace_speed = self._spots[spot_id].maintenance_speed
            for planned_booking in spot_schedule:
                # check whether maintenance process can be finished before next booking
                possible_end_time = possible_start_time + spot_maintenance_duration
                pb_start_time, pb_et, pb_id = planned_booking
                if possible_end_time <= pb_start_time:
                    list_station_offers.append((self.id, spot_id, possible_start_time, possible_end_time, desired_end_cleanliness, maintenenace_speed))
                    found_free_slot = True
                    break
                if pb_et > possible_start_time:
                    possible_start_time = pb_et
            if not found_free_slot:
                possible_end_time = possible_start_time + spot_maintenance_duration
                list_station_offers.append((self.id, spot_id, possible_start_time, possible_end_time, desired_end_cleanliness, maintenenace_speed))
                if possible_start_time == planned_arrival_time and len(list_station_offers) >= max_offers_per_station:
                    LOG.debug("early brake in offer search")
                    break
            LOG.debug(f"possible slots for station {self.id} at spot {spot_id}:")
            LOG.debug(f"    -> {list_station_offers}")
        # TODO # check methodology to stop
        if len(list_station_offers) > max_offers_per_station:
            # only keep offer with earliest start
            list_station_offers = sorted(list_station_offers, key=lambda x: x[2])[:max_offers_per_station]
        return list_station_offers
    
    def add_external_booking(self, start_time, end_time, sim_time, veh_struct):
        """ this methods adds an external booking to the maintenance station and therefor occupies a spot for a given time
        :param start_time: start time of booking
        :param end_time: end time of booking
        :param sim_time : current simulation time
        :return: True, if booking is possible; False if all spots are already occupied"""
        found_spot = None
        for spot_id, spot_schedule in self.get_current_schedules(sim_time).items():
            if len(spot_schedule) == 0:
                found_spot = spot_id
                break
            else:
                last_end_time = start_time
                for pb_start_time, pb_end_time, _ in sorted(spot_schedule,key=lambda x:x[0]):
                    if pb_start_time >= end_time and last_end_time <= start_time:
                        found_spot = spot_id
                        last_end_time = pb_end_time
                        break
                    if pb_start_time >= end_time:
                        last_end_time = pb_end_time
                        break
                    last_end_time = pb_end_time
                if found_spot is not None:
                    break
                if last_end_time <= start_time:
                    found_spot = spot_id
                    break
        if found_spot is None:
            return False
        else:
            self.make_booking(sim_time, found_spot, veh_struct, start_time=start_time, end_time=end_time)
            return True

    def __append_to_history(self, sim_time, vehicle, event_name, spot=None):
        if self.station_history_file_path is not None:
            self.station_history["time"].append(sim_time)
            self.station_history["event"].append(event_name)
            self.station_history["station_id"].append(self.id)
            self.station_history["clean_op_id"].append(self.clean_op_id)
            self.station_history["vid"].append(vehicle.vid)
            self.station_history["op_id"].append(vehicle.op_id)
            self.station_history["veh_type"].append(vehicle.veh_type)
            self.station_history["current_cleanliness"].append(round(vehicle.cleanliness, 3))
            self.station_history["spot_id"].append("" if spot is None else spot.id)
            self.station_history["spot_maintenance_speed"].append("" if spot is None else spot.maintenance_speed)
            self.station_history["initial_cleanliness"].append("" if spot is None else round(spot.initial_cleanliness, 3))
            self.station_history["cleaned_amount"].append("" if spot is None else spot.cleaned_amount)
            self.station_history["connection_duration"].append("" if spot is None else sim_time - spot.connect_time)
            if len(self.station_history) > 0:
                self.write_history_to_file()

    def add_final_states_to_history(self, sim_time, vehicle):
        spot = self._vid_spot_dict[vehicle.vid]
        self.__append_to_history(sim_time, vehicle, "final_state", spot)

    @staticmethod
    def write_history_to_file():
        file = Path(MaintenanceStation.station_history_file_path)
        if len(MaintenanceStation.station_history) > 0:
            df = DataFrame(MaintenanceStation.station_history)
            df.to_csv(file, index=False, mode="a", header=not file.exists())
            MaintenanceStation.station_history = defaultdict(list)

    @staticmethod
    def set_history_file_path(path):
        MaintenanceStation.station_history_file_path = Path(path)


# TODO # keep track of parking (inactive) vehicles | query parking lots
class Depot(MaintenanceStation):
    """This class represents a maintenance station with parking lots for inactive vehicles."""
    def __init__(self, station_id, clean_op_id, node, spot_ids, maintenance_speeds: List[float], number_parking_spots):
        super().__init__(station_id, clean_op_id, node, spot_ids, maintenance_speeds)
        self.number_parking_spots = number_parking_spots
        self.deactivated_vehicles: tp.List[SimulationVehicle] = []
        
    @property
    def free_parking_spots(self):
        return self.number_parking_spots - len(self.deactivated_vehicles)
    
    @property
    def parking_vehicles(self):
        return len(self.deactivated_vehicles)
        
    def schedule_inactive(self, veh_obj):
        """ adds the vehicle to park at the depot
        :param veh_obj: vehicle obj"""
        LOG.debug(f"park vid {veh_obj.vid} in depot {self.id} with parking vids {[x.vid for x in self.deactivated_vehicles]}")
        self.deactivated_vehicles.append(veh_obj)
        
    def schedule_active(self, veh_obj):
        """ removes the vehicle from the depot
        :param veh_obj: vehicle obj"""
        LOG.debug(f"activate vid {veh_obj.vid} in depot {self.id} with parking vids {[x.vid for x in self.deactivated_vehicles]}")
        self.deactivated_vehicles.remove(veh_obj)
        
    def pick_vehicle_to_be_active(self) -> SimulationVehicle:
        """ selects the vehicle with highest cleanliness from the list of deactivated vehicles (does not activate the vehicle yet!)
        :return: simulation vehicle obj"""
        return max([veh for veh in self.deactivated_vehicles if veh.pos == self.pos], key = lambda x:x.cleanliness)
    
    def refill_maintenance(self, fleetctrl: FleetControlBase, simulation_time, keep_free_for_short_term=0):
        """This method fills empty maintenance slots in a depot with the lowest cleanliness parking (status 5) vehicles.
        The vehicles receive a locked MaintenancePlanStop , which will be followed by another inactive planstop.
        These will directly be assigned to the vehicle. The maintenance process is directly booked with a spot.

        :param fleetctrl: FleetControl class
        :param simulation_time: current simulation time
        :param keep_free_for_short_term: optional parameter in order to keep short-term maintenance capacity (Not implemented yet)
        :return: None
        """
        if keep_free_for_short_term != 0:
            raise NotImplementedError("keep free for short term is not implemented yet!")
        # check for vehicles that require maintenance
        list_consider_maintenance: List[SimulationVehicle] = []
        for veh_obj in self.deactivated_vehicles:
            if veh_obj.cleanliness == 1.0 or veh_obj.status != VRL_STATES.OUT_OF_SERVICE:
                continue
            # check whether veh_obj already has vcl
            consider_maintenance = True
            for vrl in veh_obj.assigned_route:
                if vrl.status == VRL_STATES.CHARGING:
                    consider_maintenance = False
                    break
            if consider_maintenance:
                list_consider_maintenance.append(veh_obj)
        if not list_consider_maintenance:
            return

        for veh_obj in sorted(list_consider_maintenance, key = lambda x:x.cleanliness):
            maintenance_options = self.get_maintenance_slots(simulation_time, veh_obj, simulation_time, veh_obj.cleanliness, 1.0)
            if len(maintenance_options) > 0:
                selected_maintenance_option = min(maintenance_options, key=lambda x:x[3])
                ch_process = self.make_booking(simulation_time, selected_maintenance_option[1], veh_obj, start_time=selected_maintenance_option[2], end_time=selected_maintenance_option[3])
                start_time, end_time = ch_process.get_scheduled_start_end_times()
                maintenance_task_id = (self.clean_op_id, ch_process.id)
                ch_ps = MaintenancePlanStop(self.pos, maintenance_task_id=maintenance_task_id, earliest_start_time=start_time, duration=end_time-start_time,
                                         maintenance_speed=selected_maintenance_option[5], locked=True)
                
                assert fleetctrl.veh_plans[veh_obj.vid].list_plan_stops[-1].get_state() == G_PLANSTOP_STATES.INACTIVE
                if start_time == simulation_time:
                    LOG.debug(" -> start now")
                    # finish current status 5 task
                    veh_obj.end_current_leg(simulation_time)
                    # modify veh-plan: insert maintenance before list position -1
                    fleetctrl.veh_plans[veh_obj.vid].add_plan_stop(ch_ps, veh_obj, simulation_time,
                                                                    fleetctrl.routing_engine,
                                                                    return_copy=False, position=-1)
                    fleetctrl.lock_current_vehicle_plan(veh_obj.vid)
                    # assign vehicle plan
                    fleetctrl.assign_vehicle_plan(veh_obj, fleetctrl.veh_plans[veh_obj.vid], simulation_time, assigned_maintenance_task=(maintenance_task_id, ch_process))
                else:
                    LOG.debug(" -> start later")
                    # modify veh-plan:
                    # finish current inactive task
                    _, inactive_vrl = veh_obj.end_current_leg(simulation_time)
                    fleetctrl.receive_status_update(veh_obj.vid, simulation_time, [inactive_vrl])
                    # add new inactivate task with corresponding duration
                    inactive_ps_1 = RoutingTargetPlanStop(self.pos, locked=True, duration=start_time - simulation_time, planstop_state=G_PLANSTOP_STATES.INACTIVE)
                    # add inactivate task after maintenance
                    inactive_ps_2 = RoutingTargetPlanStop(self.pos, locked=True, duration=LARGE_INT, planstop_state=G_PLANSTOP_STATES.INACTIVE)
                    # new veh plan
                    new_veh_plan = VehiclePlan(veh_obj, simulation_time, fleetctrl.routing_engine, [inactive_ps_1, ch_ps, inactive_ps_2])
                    
                    fleetctrl.lock_current_vehicle_plan(veh_obj.vid)
                    # assign vehicle plan
                    fleetctrl.assign_vehicle_plan(veh_obj, new_veh_plan, simulation_time, assigned_maintenance_task=(maintenance_task_id, ch_process))

class PublicMaintenanceInfrastructureOperator:

    def __init__(self, clean_op_id: int, public_maintenance_station_file: str, ch_operator_attributes: dict,
                 scenario_parameters: dict, dir_names: dict, routing_engine: NetworkBase, initial_maintenance_events_f: str = None):
        """This class represents the operator for the maintenance infrastructure.

        :param clean_op_id: id of maintenance operator
        :param public_maintenance_station_file: path to file where maintenance stations are loaded from
        :param ch_operator_attributes: dictionary that can contain additionally required parameters (parameter specific for maintenance operator)
        :param scenario_parameters: dictionary that contain global scenario parameters
        :param dir_names: dictionary that specifies the folder structure of the simulation
        :param routing_engine: reference to network class
        :param initial_maintenance_events_f: in this file maintenance events are specified that are booked at the beginning of the simulation
        """

        self.clean_op_id = clean_op_id
        self.routing_engine = routing_engine
        self.ch_operator_attributes = ch_operator_attributes
        self.maintenance_stations: tp.List[MaintenanceStation] = self._loading_maintenance_stations(public_maintenance_station_file, dir_names)
        self.station_by_id: tp.Dict[int, MaintenanceStation] = {station.id: station for station in self.maintenance_stations}
        self.pos_to_list_station_id: tp.Dict[tuple, tp.List[int]] = {}
        for station_id, station in self.station_by_id.items():
            try:
                self.pos_to_list_station_id[station.pos].append(station_id)
            except KeyError:
                self.pos_to_list_station_id[station.pos] = [station_id]
                
        self.max_search_radius = scenario_parameters.get(G_CH_OP_MAX_STATION_SEARCH_RADIUS)
        self.max_considered_stations = scenario_parameters.get(G_CH_OP_MAX_CHARGING_SEARCH, 100)
        
        sim_start_time = scenario_parameters[G_SIM_START_TIME]
        sim_end_time = scenario_parameters[G_SIM_END_TIME]
        
        self.sim_time_step = scenario_parameters[G_SIM_TIME_STEP]
        
        if initial_maintenance_events_f is not None:
            class VehicleStruct():
                def __init__(self) -> None:
                    self.vid = -1
            
            maintenance_events = pd.read_csv(initial_maintenance_events_f)
            for station_id, start_time, end_time in zip(maintenance_events["maintenance_station_id"].values, maintenance_events["start_time"].values, maintenance_events["end_time"].values):
                if end_time < sim_start_time or start_time > sim_end_time:
                    continue
                self.station_by_id[station_id].add_external_booking(start_time, end_time, sim_start_time, VehicleStruct())
                

    def _loading_maintenance_stations(self, public_maintenance_station_file, dir_names) -> List[MaintenanceStation]:
        """ Loads the maintenance stations from the provided csv file"""
        stations_df = pd.read_csv(public_maintenance_station_file)
        file = Path(dir_names[G_DIR_OUTPUT]).joinpath("5_maintenance_stats.csv")
        if file.exists():
            file.unlink()
        MaintenanceStation.set_history_file_path(file)
        stations = []
        for _, row in stations_df.iterrows():
            station_id = row[G_INFRA_MS_ID]
            node_index = row[G_NODE_ID]
            cunit_dict = decode_config_str(row[G_INFRA_MU_DEF])
            if cunit_dict is None:
                cunit_dict = {}
            spot_ids = [i for i in range(sum(cunit_dict.values()))]
            spot_maintenance_speeds = []
            for maintenance_speed, number in cunit_dict.items():
                spot_maintenance_speeds += [maintenance_speed for _ in range(number)]
            stations.append(MaintenanceStation(station_id, self.clean_op_id, node_index, spot_ids, spot_maintenance_speeds))
        return stations

    def modify_booking(self, sim_time, booking: MaintenanceProcess):
        booking.station.modify_booking(sim_time, booking)

    def cancel_booking(self, sim_time, booking: MaintenanceProcess):
        booking.station.cancel_booking(sim_time, booking)

    def book_station(self, sim_time, vehicle: SimulationVehicle, station_id, spot_id, start_time=None,
                     end_time=None) -> MaintenanceProcess:
        """ Books a spot at the maintenance station """

        station = self.station_by_id[station_id]
        return station.make_booking(sim_time, spot_id, vehicle, start_time, end_time)

    def get_maintenance_slots(self, sim_time, vehicle: SimulationVehicle, planned_start_time, planned_veh_pos,
                           planned_veh_cleanliness, desired_veh_cleanliness, max_number_maintenance_stations=1, max_offers_per_station=1) \
            -> tp.List[tp.Tuple[int, int, int, int, float, float, float]]:
        """ Returns specific maintenance possibilities for a vehicle at a future time, place with estimated cleanliness and desired cleanliness.

        :param sim_time: current simulation time
        :param vehicle: the vehicle for which the maintenance slot is required
        :param planned_start_time: earliest time at which maintenance should be considered
        :param planned_veh_pos: time from which vehicle will drive to maintenance station
        :param planned_veh_cleanliness: estimated vehicle cleanliness at that position
        :param desired_veh_cleanliness: desired final cleanliness after maintenance
        :param max_number_maintenance_stations: maximum number of maintenance stations to consider
        :param max_offers_per_station: maximum number of offers per maintenance station to consider
        :return: list of specific offers of MaintenanceOperator consisting of
                    (maintenance station id, maintenance spot id, booking start time, booking end time, expected booking end cleanliness, max maintenance speed)
        """
        considered_station_list = self._get_considered_stations(planned_veh_pos)
        list_offers = []
        c = 0
        for station_id, tt, dis in considered_station_list:
            station = self.station_by_id[station_id]
            estimated_arrival_time = planned_start_time + tt
            list_station_offers = station.get_maintenance_slots(sim_time, vehicle, estimated_arrival_time, planned_veh_cleanliness, desired_veh_cleanliness, max_offers_per_station=max_offers_per_station)
            LOG.debug(f"possible maintenance offers from station {station_id} for veh {vehicle.vid} with tt {tt} : {planned_start_time} -> {estimated_arrival_time}")
            LOG.debug(f"{list_station_offers}")
            list_offers.extend(list_station_offers)
            # TODO # check methodology to stop
            if len(list_station_offers) > 0:
                c += 1
                if c == max_number_maintenance_stations:
                    break
        return list_offers

    def _get_considered_stations(self, position: tuple) -> tp.List[tuple]:
        """ Returns the list of stations nearest stations (using euclidean distance) within search radius of the
        position.

        :param position: position around which the station is sought
        :returns:   List of tuple maintenance station id, travel time from position, travel distanc from position
                        in order of proximity to the provided position
        """
        r_list = self.routing_engine.return_travel_costs_1toX(position, self.pos_to_list_station_id.keys(),
                                                        max_routes=self.max_considered_stations, max_cost_value=self.max_search_radius)
        r = []
        c = 0
        for d_pos, _, tt, dis in r_list:
            for station_id in self.pos_to_list_station_id[d_pos]:
                r.append( (station_id, tt, dis) )
                c += 1
                if c == self.max_considered_stations:
                    return r
        return r
    
    def _remove_unrealized_bookings(self, sim_time):
        """ this method removes all planned bookings that are not ended by the update of a simulation vehicle and are there considered as not realized
        :param sim_time: simulation time"""
        for s_id, maintenance_station in self.station_by_id.items():
            schedule_dict = maintenance_station.get_current_schedules(sim_time)
            running_processes = maintenance_station.get_running_processes()[0]
            for spot_id, schedule in schedule_dict.items():
                if len(schedule) > 0:
                    start_time, end_time, booking_id = min(schedule, key=lambda x:x[1])
                    end_booking_flag = False
                    if end_time <= sim_time:
                        end_booking_flag = True
                    if end_time - start_time > self.sim_time_step and end_time <= sim_time + self.sim_time_step:
                        end_booking_flag = True
                    if end_booking_flag:
                        if running_processes.get(spot_id) is None or running_processes.get(spot_id).id != booking_id:
                            LOG.debug("end unrealized booking at time {} at station {} spot {}: {}".format(sim_time, s_id, spot_id, booking_id))
                            try:
                                ch_process = maintenance_station._booked_processes[booking_id]
                                maintenance_station.cancel_booking(sim_time, ch_process)
                            except KeyError:
                                LOG.warning("couldnt cancel maintenance booking {}".format(booking_id))
                        
    
    def time_trigger(self, sim_time):
        """ this method is triggered in each simulation time step
        :param sim_time: simulation time"""
        t = time.time()
        self._remove_unrealized_bookings(sim_time)
        LOG.debug("maintenance infra time trigger took {}".format(time.time() - t))
    

class OperatorMaintenanceAndDepotInfrastructure(PublicMaintenanceInfrastructureOperator):
    """ this class has similar functionality like a MaintenanceInfrastructureOperator but is unique for each MoD operator (only the corresponding operator
    has access to the maintenance stations """
    # TODO # functionality for parking lots here | functionality for activating/deactivating in fleetctrl/fleetsizing
    # TODO # functionality for depot maintenance in fleetctrl/maintenance
    def __init__(self, op_id: int, depot_file: str, operator_attributes: dict,
                 scenario_parameters: dict, dir_names: dict, routing_engine: NetworkBase):
        """This class represents the operator for the maintenance infrastructure.

        :param op_id: id of mod operator this depot class belongs to
        :param depot_file: path to file where maintenance stations an depots are loaded from
        :param operator_attributes: dictionary that can contain additionally required parameters (parameter specific for the mod operator)
        :param scenario_parameters: dictionary that contain global scenario parameters
        :param dir_names: dictionary that specifies the folder structure of the simulation
        :param routing_engine: reference to network class
        """
        super().__init__(f"op_{op_id}", depot_file, operator_attributes, scenario_parameters, dir_names, routing_engine)
        self.depot_by_id: tp.Dict[int, Depot] = {depot_id : depot for depot_id, depot in self.station_by_id.items() if depot.number_parking_spots > 0}
        
    def _loading_maintenance_stations(self, depot_file, dir_names) -> List[Depot]:
        """ Loads the maintenance stations from the provided csv file"""
        stations_df = pd.read_csv(depot_file)
        file = Path(dir_names[G_DIR_OUTPUT]).joinpath("5_maintenance_stats.csv")
        if file.exists():
            file.unlink()
        MaintenanceStation.set_history_file_path(file)
        stations = []
        for _, row in stations_df.iterrows():
            station_id = row[G_INFRA_MS_ID]
            node_index = row[G_NODE_ID]
            cunit_dict = decode_config_str(row[G_INFRA_MU_DEF])
            number_parking_spots = row[G_INFRA_MAX_PARK]
            if cunit_dict is None:
                cunit_dict = {}
            spot_ids = [i for i in range(sum(cunit_dict.values()))]
            spot_maintenance_speeds = []
            for maintenance_speed, number in cunit_dict.items():
                spot_maintenance_speeds += [maintenance_speed for _ in range(number)]
            stations.append(Depot(station_id, self.clean_op_id, node_index, spot_ids, spot_maintenance_speeds, number_parking_spots))
        return stations
    
    def find_nearest_free_depot(self, pos, check_free=True) -> Depot:
        """This method can be used to send a vehicle to the next depot.

        :param pos: final vehicle position
        :param check_free: if set to False, the check for free parking is ignored
        :return: Depot
        """
        free_depot_positions = {}
        for depot in self.depot_by_id.values():
            if depot.free_parking_spots > 0 or not check_free:
                free_depot_positions[depot.pos] = depot
        re_list = self.routing_engine.return_travel_costs_1toX(pos, free_depot_positions.keys(), max_routes=1)
        if re_list:
            destination_pos = re_list[0][0]
            depot = free_depot_positions[destination_pos]
        else:
            depot = None
        return depot
    
    def time_trigger(self, sim_time):
        super().time_trigger(sim_time)

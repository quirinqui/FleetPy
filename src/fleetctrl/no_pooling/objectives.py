from src.misc.globals import *

LARGE_INT = 1000000


# -------------------------------------------------------------------------------------------------------------------- #
# main function
# -------------
def return_objective_function(vr_control_func_dict):
    """This function generates the control objective functions for vehicle-request assignment in non pooling operation.
    The control objective functions contain an assignment reward of LARGE_INT and are to be
    ---------------
    -> minimized <-
    ---------------

    :param vr_control_func_dict: dictionary which has to contain "func_key" as switch between possible functions;
            additional parameters of a function can have additional keys.
    :type vr_control_func_dict: dict
    :return: objective function
    :rtype: function
    """
    func_key = vr_control_func_dict["func_key"]

    # ---------------------------------------------------------------------------------------------------------------- #
    # control objective function definitions
    # --------------------------------------


    if func_key == "distance_and_user_times_with_walk":
        traveler_vot = vr_control_func_dict["vot"]

        def control_f(simulation_time, veh_obj, veh_plan, rq_dict, routing_engine):
            """This function combines the total driving costs and the value of customer time.

            :param simulation_time: current simulation time
            :param veh_obj: simulation vehicle object
            :param veh_plan: vehicle plan in question
            :param rq_dict: rq -> Plan request dictionary
            :param routing_engine: for routing queries
            :return: objective function value
            """
            assignment_reward = len(veh_plan.pax_info) * LARGE_INT
            # distance term
            sum_dist = 0
            last_pos = veh_obj.pos
            for ps in veh_plan.list_plan_stops:
                pos = ps.get_pos()
                if pos != last_pos:
                    sum_dist += routing_engine.return_travel_costs_1to1(last_pos, pos)[2]
                    last_pos = pos
            # value of time term (treat waiting and in-vehicle time the same)
            sum_user_times = 0
            for rid, boarding_info_list in veh_plan.pax_info.items():
                rq_time = rq_dict[rid].rq_time
                walking_time_end = rq_dict[rid].walking_time_end    #walking time start allready included in interval rq-time -> drop_off_time
                drop_off_time = boarding_info_list[1]
                sum_user_times += (drop_off_time - rq_time) + walking_time_end
            # vehicle costs are taken from simulation vehicle (cent per meter)
            # value of travel time is scenario input (cent per second)
            return sum_dist * veh_obj.distance_cost + sum_user_times * traveler_vot - assignment_reward
    
    elif func_key == "IRS_study_standard":
        def control_f(simulation_time, veh_obj, veh_plan, rq_dict, routing_engine):
            """This function tries to minimize the waiting time of unlocked users.

            :param simulation_time: current simulation time
            :param veh_obj: simulation vehicle object
            :param veh_plan: vehicle plan in question
            :param rq_dict: rq -> Plan request dictionary
            :param routing_engine: for routing queries
            :return: objective function value
            """
            sum_user_wait_times = 0
            assignment_reward = 0
            sum_dist = 0
            last_pos = veh_obj.pos
            for ps in veh_plan.list_plan_stops:
                pos = ps.get_pos()
                if pos != last_pos and len(ps.get_list_boarding_rids()):
                    sum_dist += routing_engine.return_travel_costs_1to1(last_pos, pos)[2]
                    last_pos = pos
            for rid, boarding_info_list in veh_plan.pax_info.items():
                prq = rq_dict[rid]
                if prq.pu_time is None:
                    rq_time = rq_dict[rid].rq_time
                    pick_up_time = boarding_info_list[0]
                    sum_user_wait_times += (pick_up_time - rq_time)
                    if prq.is_locked():
                        assignment_reward += LARGE_INT*10000
                    elif prq.status < G_PRQS_LOCKED:
                        assignment_reward += float(LARGE_INT)
                        # TODO # Cplex only allows floats. Therefore this workaround.
                        #  For some reason LARGE_INT*10000 does not seem to be a problem though...
                    else:
                        assignment_reward += LARGE_INT*100
            # 4 is the empirically found parameter to weigh saved dist against saved waiting time
            return sum_dist + sum_user_wait_times - assignment_reward

    else:
        raise IOError(f"Did not find valid request assignment control objective string."
                      f" Please check the input parameter {G_OP_VR_CTRL_F}!")

    return control_f

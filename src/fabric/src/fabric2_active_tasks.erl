-module(fabric2_active_tasks).


-export([
    get_active_tasks/0,
    register_tasks/1
]).


-include_lib("couch/include/couch_db.hrl").
-include("fabric2.hrl").


-define(ACTIVE_TASK_JOB_TYPES, [<<"views">>, <<"replication">>]).


register_tasks(Mod) when is_atom(Mod) ->
    ActiveTasks = lists:usort([Mod | registrations()]),
    application:set_env(fabric, active_tasks, ActiveTasks).


get_active_tasks() ->
    ActiveTasks = lists:foldl(fun(Type, TaskAcc) ->
        Tasks = couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(undefined),
            fun(JTx) ->
                JobIds = couch_jobs:get_active_jobs(JTx, Type),
                maps:map(fun(JobId, _) ->
                    {ok, Data} = couch_jobs:get_job_data(JTx, Type, JobId),
                    populate_active_tasks(Type, Data)
                end, JobIds)
            end),
        maps:merge(TaskAcc, Tasks)
    end, #{}, ?ACTIVE_TASK_JOB_TYPES),
    maps:values(ActiveTasks).


populate_active_tasks(Type, Data) ->
    lists:foldl(fun(Mod, TaskAcc) ->
        Tasks = Mod:populate_active_tasks(Type, Data),
        maps:merge(TaskAcc, Tasks)
    end, #{}, registrations()).


registrations() ->
    application:get_env(fabric, active_tasks, []).

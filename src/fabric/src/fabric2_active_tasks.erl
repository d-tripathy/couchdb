-module(fabric2_active_tasks).


-export([
    get_active_tasks/0
]).


-include_lib("couch/include/couch_db.hrl").
-include("fabric2.hrl").


-define(ACTIVE_TASK_JOB_TYPES, [<<"views">>, <<"replication">>]).


get_active_tasks() ->
    ActiveTasks = lists:foldl(fun(Type, TaskAcc) ->
        Tasks = couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(undefined),
            fun(JTx) ->
                JobIds = couch_jobs:get_active_jobs(JTx, Type),
                maps:map(fun(JobId, _) ->
                    {ok, Data} = couch_jobs:get_job_data(JTx, Type, JobId),
                    maps:get(<<"active_tasks_info">>, Data #{})
                end, JobIds)
            end),
        maps:merge(TaskAcc, Tasks)
    end, #{}, ?ACTIVE_TASK_JOB_TYPES),
    maps:values(ActiveTasks).

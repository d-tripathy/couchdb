-module(fabric2_active_tasks).


-export([
    get_active_tasks/0,
    get_active_task_info/1,

    update_active_task_info/2
]).


-define(ACTIVE_TASK_INFO, <<"active_task_info">>).


get_active_tasks() ->
    ActiveTasks = couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(undefined),
        fun(JTx) ->
            Types = couch_jobs:get_types(JTx),
            lists:foldl(fun(Type, TaskAcc) ->
                JobIds = couch_jobs:get_active_jobs(JTx, Type),
                Tasks = maps:map(fun(JobId, _) ->
                    {ok, Data} = couch_jobs:get_job_data(JTx, Type, JobId),
                    maps:get(?ACTIVE_TASK_INFO, Data #{})
                end, JobIds),
                maps:merge(TaskAcc, Tasks)
            end, #{}, Types)
        end),
    maps:values(ActiveTasks).


update_active_task_info(JobData, ActiveTaskInfo) ->
     JobData#{?ACTIVE_TASK_INFO => ActiveTaskInfo}.


get_active_task_info(JobData) ->
    #{?ACTIVE_TASK_INFO:= ActiveTaskInfo} = JobData,
    ActiveTaskInfo.

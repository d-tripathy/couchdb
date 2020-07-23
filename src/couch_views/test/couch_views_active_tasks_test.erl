% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_views_active_tasks_test).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("couch_views/include/couch_views.hrl").
-include_lib("fabric/test/fabric2_test.hrl").


-define(MAP_FUN1, <<"map_fun1">>).
-define(TOTAL_DOCS, 1000).


setup() ->
    Ctx = test_util:start_couch([
            fabric,
            couch_jobs,
            couch_js,
            couch_views
        ]),
    Ctx.


cleanup(Ctx) ->
    test_util:stop_couch(Ctx).


foreach_setup() ->
    {ok, Db} = fabric2_db:create(?tempdb(), [{user_ctx, ?ADMIN_USER}]),
    
    DDoc = create_ddoc(),
    Docs = make_docs(?TOTAL_DOCS),
    fabric2_db:update_docs(Db, [DDoc | Docs]),

    {Db, DDoc}.


foreach_teardown({Db, _}) ->
    meck:unload(),
    ok = fabric2_db:delete(fabric2_db:name(Db), []).


active_tasks_test_() ->
    {
        "Active Tasks test",
        {
            setup,
            fun setup/0,
            fun cleanup/1,
            {
                foreach,
                fun foreach_setup/0,
                fun foreach_teardown/1,
                [
                    ?TDEF_FE(verify_basic_active_tasks)
                ]
            }
        }
    }.


verify_basic_active_tasks({Db, DDoc}) ->
    pause_indexer_for_changes(self()),
    couch_views:build_indices(Db, [DDoc]),
    {Pid, {changes_done, ChangesDone}} = wait_to_reach_changes(10000),
    Pid ! continue,
    ?assertEqual(ChangesDone, ?TOTAL_DOCS).


create_ddoc() ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/bar">>},
        {<<"views">>, {[
            {?MAP_FUN1, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc.val);}">>}
            ]}}
        ]}}
    ]}).


doc(Id, Val) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(integer_to_list(Id))},
        {<<"val">>, Val}
    ]}).


make_docs(Count) ->
    [doc(I, Count) || I <- lists:seq(1, Count)].


pause_indexer_for_changes(ParentPid) ->
    meck:new(couch_views_util, [passthrough]),
    meck:expect(couch_views_util, active_tasks_info, fun(ChangesDone,
        DbName, DDocId, LastSeq, DBSeq) ->
        case ChangesDone of
            ?TOTAL_DOCS ->
                ParentPid ! {self(), {changes_done, ChangesDone}},
                receive continue -> ok end;
            _ ->
                ok
        end,
        meck:passthrough([ChangesDone, DbName, DDocId, LastSeq,
            DBSeq])
    end).


wait_to_reach_changes(Timeout) ->
    receive
        {Pid, {changes_done, ChangesDone}} when is_pid(Pid) ->
            {Pid, {changes_done, ChangesDone}}
    after Timeout ->
        error(timeout_in_pause_indexer_for_changes)
    end.

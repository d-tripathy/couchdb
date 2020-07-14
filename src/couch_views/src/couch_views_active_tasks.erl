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


-module(couch_views_active_tasks).


-export([
    populate_active_tasks/2
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").


populate_active_tasks(<<"views">>, Data)  ->
    #{
        <<"db_name">> := DbName,
        <<"ddoc_id">> := DDocId,
        <<"changes_done">> := ChangesDone,
        <<"db_uuid">> := DbUUID
    } = Data,
    
    {ok, Db} = try
        fabric2_db:open(DbName, [?ADMIN_CTX, {uuid, DbUUID}])
    catch error:database_does_not_exist ->
        {ok, not_found}
    end,

    {ok, DDoc} = case fabric2_db:open_doc(Db, DDocId) of
        {ok, DDoc0} ->
            {ok, DDoc0};
        {not_found, _} ->
            {ok, not_found}
    end,
    
    case {Db, DDoc} of
        {not_found, _} ->
            #{};
        {_, not_found} ->
            #{};
        {_, _} ->
            {ok, Mrst} = couch_views_util:ddoc_to_mrst(DbName, DDoc),
            {ViewSeq, DBSeq} = fabric2_fdb:transactional(Db, fun(TxDb) ->
                VSeq = couch_views_fdb:get_update_seq(TxDb, Mrst),
                DBSeq0 = fabric2_fdb:get_last_change(TxDb),
                {VSeq, DBSeq0}
            end),
            VStamp = convert_seq_to_stamp(ViewSeq),
            DBStamp = convert_seq_to_stamp(DBSeq),
            #{
                <<"database">> => DbName,
                <<"changes_done">> => ChangesDone,
                <<"design_document">> => DDocId,
                <<"current_version_stamp">> => VStamp,
                <<"db_version_stamp">> => DBStamp
            }
    end.

populate_active_task(_ , _Data) ->
    #{}.


% move this over to util
convert_seq_to_stamp(<<"0">>) ->
    <<"0-0-0">>;

convert_seq_to_stamp(undefined) ->
    <<"0-0-0">>;

convert_seq_to_stamp(Seq) ->
    {_, Stamp, Batch, DocNumber} = fabric2_fdb:seq_to_vs(Seq),
    VS = integer_to_list(Stamp) ++ "-" ++ integer_to_list(Batch)
        ++ "-" ++ integer_to_list(DocNumber),
    list_to_binary(VS).

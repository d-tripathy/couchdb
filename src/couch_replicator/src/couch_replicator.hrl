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

-define(REP_ID_VERSION, 4).

% Couch jobs types and timeouts
-define(REP_DOCS, <<"repdocs">>).
-define(REP_JOBS, <<"repjobs">>).
-define(REP_DOCS_TIMEOUT_MSEC, 17000).
-define(REP_JOBS_TIMEOUT_MSEC, 33000).

% Some fields from the replication doc
-define(SOURCE, <<"source">>).
-define(TARGET, <<"target">>).
-define(CREATE_TARGET, <<"create_target">>).
-define(DOC_IDS, <<"doc_ids">>).
-define(SELECTOR, <<"selector">>).
-define(FILTER, <<"filter">>).
-define(QUERY_PARAMS, <<"query_params">>).
-define(URL, <<"url">>).
-define(AUTH, <<"auth">>).
-define(HEADERS, <<"headers">>).

% Replication states
-define(ST_ERROR, <<"error">>).
-define(ST_FINISHED, <<"completed">>).
-define(ST_RUNNING, <<"running">>).
-define(ST_INITIALIZING, <<"initializing">>).
-define(ST_FAILED, <<"failed">>).
-define(ST_PENDING, <<"pending">>).
-define(ST_ERROR, <<"error">>).
-define(ST_CRASHING, <<"crashing">>).
-define(ST_TRIGGERED, <<"triggered">>).

% Some fields from a rep object
-define(REP_ID, <<"id">>).
-define(DB_NAME, <<"db_name">>).
-define(DOC_ID, <<"doc_id">>).
-define(START_TIME, <<"start_time">>).

% Fields couch job data objects
-define(REP, <<"rep">>).
-define(REP_PARSE_ERROR, <<"rep_parse_error">>).
-define(STATE, <<"state">>).
-define(STATE_INFO, <<"state_info">>).
-define(DOC_STATE, <<"doc_state">>).
-define(DB_NAME, <<"db_name">>).
-define(DOC_ID, <<"doc_id">>).
-define(ERROR_COUNT, <<"error_count">>).
-define(LAST_UPDATED, <<"last_updated">>).
-define(HISTORY, <<"history">>).
-define(VER, <<"ver">>).

% Accepted job message tag
-define(ACCEPTED_JOB, accepted_job).



-type rep_id() :: binary().
-type user_name() :: binary() | null.
-type db_doc_id() :: {binary(), binary() | '_'}.
-type seconds() :: non_neg_integer().

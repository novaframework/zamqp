-module(zamqp_pool_workers_sup).

-behaviour(supervisor).

%% Master API
-export([start_child/6]).

%% Management API
-export([start_link/1]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% Master API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_child() -> {ok, Pid}
%% @doc
%%   Starts the
%% @end
%%--------------------------------------------------------------------
-spec start_child(pid(), binary(), json:json(), binary(), _, _) -> {ok, pid()}.
%%--------------------------------------------------------------------
start_child(Sup, Key, JSON, Tag, Log, Con) ->
    supervisor:start_child(Sup, [Key, JSON, Tag, Log, Con]).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the main supervisor of the zamqp application.
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}) -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link(Consumer) -> supervisor:start_link(?MODULE, Consumer).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init(#{}) -> {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init(Consumer = #{module := Module, queue := Queue}) ->
    zamqp_pool_master:sup({Module, Queue}),
    {ok,
     {{simple_one_for_one, 100, 100},
      [child(zamqp_pool_worker, [Consumer])]}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

child(Module, Args) ->
    {Module, {Module, start_link, Args}, temporary,brutal_kill,worker,[Module]}.

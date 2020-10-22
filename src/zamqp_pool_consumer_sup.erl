-module(zamqp_pool_consumer_sup).

-behaviour(supervisor).

%% Management API
-export([start_link/2]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the main supervisor of the zamqp application.
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}, [#{}]) -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link(Consumer, Servers) ->
    supervisor:start_link(?MODULE, {Consumer, Servers}).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({#{}, [#{}]}) ->
          {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init({Consumer, Servers}) ->
    {ok,
     {{rest_for_one, 2, 3600},
      [child(zamqp_pool_master, worker, [Consumer]),
       child(zamqp_pool_consumers_sup, supervisor, [Consumer, Servers]),
       child(zamqp_pool_workers_sup, supervisor, [Consumer])]}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

child(Module, worker, Args) ->
    {Module, {Module, start_link, Args}, permanent, 5000, worker, [Module]};
child(Module, supervisor, Args) ->
    {Module, {Module, start_link, Args},permanent,infinity,supervisor,[Module]}.

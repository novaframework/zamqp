-module(zamqp_publisher_pool_sup).

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
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(#{}, [#{}]) -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link(Publisher, Servers) ->
    supervisor:start_link(?MODULE, {Publisher, Servers}).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({#{}, [#{}]}) ->
          {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init({Publisher, Servers}) ->
    {ok, {{one_for_one, 100, 100},
          [member(Server, Publisher, Server) || Server <- Servers]}}.


%% ===================================================================
%% Internal functions.
%% ===================================================================

member(Id, Publisher, Server) ->
    {Id, {zamqp_publisher, start_link, [Publisher, Server]},
     permanent, 5000, worker, [zamqp_publisher]}.

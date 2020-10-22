-module(zamqp_instance_consumer_sup).

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
    {ok, {{one_for_one, 100, 3600}, children(Consumer, Servers)}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

children(Consumer = #{module := Mod, queue := Queue}, Servers) ->
    [child({Server, Mod, Queue}, [Consumer, Server]) || Server <- Servers].

child(Id, Args) ->
    {Id,
     {zamqp_consumer, start_link, Args},
     permanent, 5000, worker, [zamqp_consumer]}.


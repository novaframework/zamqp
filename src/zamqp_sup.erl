-module(zamqp_sup).

-behaviour(supervisor).

%% Management API
-export([start_link/0, stop_consumers/0]).

%% supervisor callbacks
-export([init/1]).

%% Includes
-include_lib("zamqp.hrl").

%% ===================================================================
%% Management API
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid}
%% @doc
%%   Starts the main supervisor of the zamqp application.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, no_arg).

%%--------------------------------------------------------------------
%% Function:
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec stop_consumers() -> ok.
%%--------------------------------------------------------------------
stop_consumers() -> supervisor:terminate_child(?MODULE, zamqp_consumers_sup).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init(no_arg) -> {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init(no_arg) ->
    Servers = case application:get_env(zamqp, servers, []) of
                  [] ->?WARNING("No servers defined for ZAMQP", []);
                  Servers0 -> Servers0
              end,
    Consumers = application:get_env(zamqp, consumers, []),
    Publishers = application:get_env(zamqp, publishers, []),
    Children = [child(zamqp_reg, worker, []),
                child(zamqp_consumers_sup, [Consumers, Servers]),
                child(zamqp_publishers_sup, [Publishers, Servers])],
    {ok, {{one_for_all, 0, 1}, Children}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

child(Mod, Args) -> child(Mod, supervisor, Args).

child(Mod, worker, Args) ->
    {Mod, {Mod, start_link, Args}, permanent, brutal_kill, worker, [Mod]};
child(Mod, supervisor, Args) ->
    {Mod, {Mod, start_link, Args}, permanent, infinity, supervisor, [Mod]}.


-module(zamqp_publishers_sup).

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
-spec start_link([#{}], [#{}]) -> {ok, pid()} | ignore | {error, _}.
%%--------------------------------------------------------------------
start_link(Publishers, Servers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {Publishers, Servers}).

%% ===================================================================
%% supervisor callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec init({[#{}], [#{}]}) ->
          {ok, {supervisor:sup_flags(), [sup_flags:child_spec()]}}.
%%--------------------------------------------------------------------
init({Publishers, Servers}) ->
    {ok, {{one_for_one, 2, 3600}, children(Publishers, Servers)}}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

children(Publishers, Servers) ->
    [child(Exchange, [Publisher, Servers]) ||
        Publisher = #{exchange := Exchange} <- Publishers].

child(Id, Args) ->
    {Id, {zamqp_publisher_sup, start_link, Args},
     permanent, infinity, supervisor, [zamqp_publisher_sup]}.

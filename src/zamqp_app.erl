-module(zamqp_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%--------------------------------------------------------------------
-spec start(normal, no_arg) -> {ok, pid()}.
%%--------------------------------------------------------------------
start(normal, no_arg) -> zamqp_sup:start_link().

%%--------------------------------------------------------------------
-spec stop(_) -> ok.
%%--------------------------------------------------------------------
stop(_) -> ok.

-module(zamqp_sup_tests).

-export([handle/1]).

%% Includes
-include_lib("eunit/include/eunit.hrl").

%% Defines

%%%-------------------------------------------------------------------
%% Consumer callbacks
%%%-------------------------------------------------------------------
handle(Body) ->
    ?MODULE ! {body, Body},
    ok.

%%%-------------------------------------------------------------------
%% gen_server
%%%-------------------------------------------------------------------


gen_server_test_() ->
    {setup,
     fun setup_gen_server/0,
     fun cleanup_gen_server/1,
     [{timeout, 120, {"Start gen_server", ?_test(run_gen_server(start))}}
     ]}.

setup_gen_server() ->
    meck:new(zlog_access),
    meck:expect(zlog_access, log_event, fun(_, _, _, _) -> ok end),
    application:load({application, zamqp, []}),
    Servers = [#{user => <<"guest">>,
                 pass => <<"guest">>,
                 host => {127, 0, 0, 1},
                 port => 5672}],
    Consumers = [#{module => ?MODULE,
                   routing_key => <<"#">>,
                   queue => gen_server_test_q,
                   exchange => gen_server_test_e}],
    Publishers = [#{exchange => gen_server_test_e}],
    application:set_env(zamqp, servers, Servers),
    application:set_env(zamqp, consumers, Consumers),
    application:set_env(zamqp, publishers, Publishers),
    {ok, Pid} = zamqp_sup:start_link(),
    Pid.

cleanup_gen_server(Pid) ->
    meck:unload([zlog_access]),
    unlink(Pid),
    exit(Pid, shutdown),
    application:unload(zamqp).

run_gen_server(start) ->
    register(?MODULE, self()),
    zamqp_publisher:publish(gen_server_test_e,
                            <<"A key">>,
                            <<"A test">>,
                            <<"Test UUID">>),
    wait_for({body, <<"A test">>}),
    unregister(?MODULE).

wait_for(X) -> wait_for(X, 1000).

wait_for(X, Time) -> receive X -> ok after Time -> fail end.

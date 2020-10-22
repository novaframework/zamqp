-module(zamqp_consumer_tests).

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
    Server = #{user => <<"guest">>,
               pass => <<"guest">>,
               host => {127, 0, 0, 1},
               port => 5672},
    Consumer = #{module => ?MODULE,
                 routing_key => <<"#">>,
                 queue => gen_server_test_q,
                 exchange => gen_server_test_e},
    {ok, Pid} = zamqp_consumer:start_link(Server, Consumer),
    register(gen_server_test_server, Pid),
     Pid.

cleanup_gen_server(Pid) ->
    meck:unload([zlog_access]),
    unlink(Pid),
    exit(Pid, shutdown).

run_gen_server(start) ->
    Body = <<"A test message">>,
    send(Body),
    register(?MODULE, self()),
    sys:get_status(gen_server_test_server),
    wait_for({body, Body}),
    ok.

send(X) ->
    Method = zamqp_protocol:encode(basic,
                                   deliver,
                                  #{channel => 1,
                                    consumer_tag => <<"XX">>,
                                    delivery_tag => 4711,
                                    exchange => <<"gen_server_test_e">>,
                                    routing_key => <<"routing key">>}),
    Header = zamqp_protocol:encode(content,
                                   header,
                                   #{content_class => basic,
                                     content_body_size => byte_size(X),
                                     channel => 1}),
    Data = zamqp_protocol:encode(content, body, #{payload => X, channel => 1}),
    Frames = iolist_to_binary([Method, Header, Data]),
    gen_server_test_server ! {tcp, socket, Frames}.



wait_for(X) -> wait_for(X, 1000).

wait_for(X, Time) -> receive X -> ok after Time -> fail end.
